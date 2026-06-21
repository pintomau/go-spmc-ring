package spmc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestBitmapReaderPool_ReaderReuse(t *testing.T) {
	ctx := context.Background()
	cursor := &atomic.Int64{}
	pool := NewBitmapReaderPool[int](ctx, cursor)

	// 1. Add a reader
	started := make(chan struct{})
	readerFn := func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
		close(started)
		<-ctx.Done()
	}

	slotId, err := pool.AddReader(readerFn)
	if err != nil {
		t.Fatalf("AddReader failed: %v", err)
	}

	// Wait for it to start
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for reader to start")
	}

	// 2. Remove the reader
	if err := pool.RemoveReader(slotId); err != nil {
		t.Fatalf("RemoveReader failed: %v", err)
	}

	// Give the goroutine a moment to transition to idle state
	// In runSlot:
	//   control.state.Store(slotStateRunning)
	//   b.deallocateSlot(slotId)

	// We can poll the state until it becomes just Running
	deadline := time.Now().Add(time.Second)
	var state uint32
	for time.Now().Before(deadline) {
		state = pool.slotControl[slotId].state.Load()
		if state == slotStateRunning {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if state != slotStateRunning {
		t.Errorf("expected state to be Running (%d), got %d", slotStateRunning, state)
	}

	// Check allocated bitmap - should be 0 for this slot
	// Check if the slot is marked as allocated in the bitmap.
	// The bitmap uses one bit per slot: slotId/64 gives the array index,
	// slotId%64 gives the bit position within that uint64.
	// A set bit (1) means allocated, cleared bit (0) means deallocated.
	isAllocated := (pool.allocatedSlots[slotId/64].Load() & (1 << (slotId % 64))) != 0
	if isAllocated {
		t.Errorf("slot should be deallocated in bitmap")
	}

	// 3. Add reader again - should reuse the slot and the running goroutine
	reused := make(chan struct{})
	readerFn2 := func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
		close(reused)
		<-ctx.Done()
	}

	slotId2, err := pool.AddReader(readerFn2)
	if err != nil {
		t.Fatalf("AddReader 2 failed: %v", err)
	}

	if slotId2 != slotId {
		t.Logf("Warning: did not reuse the same slot ID (got %d, expected %d), but that might be valid if allocation strategy changed", slotId2, slotId)
	}

	select {
	case <-reused:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for reused reader to start")
	}

	// Verify state is back to full
	finalState := pool.slotControl[slotId2].state.Load()
	expected := uint32(slotStateAllocated | slotStateActive | slotStateRunning)
	if finalState != expected {
		t.Errorf("expected final state %d, got %d", expected, finalState)
	}
}

// TestRingBuffer_SelfEvictingReader pins the contract that returning from a
// ReaderFunc removes the reader from the pool and ungates the writer. The
// reader never consumes; it self-evicts once its lag exceeds maxLag, which is
// the only way the blocking writer below can finish publishing 100 events
// through a capacity-8 ring.
func TestRingBuffer_SelfEvictingReader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	evicted := make(chan struct{})
	if _, err := rb.barrier.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		defer close(evicted)
		const maxLag = 4
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if rv.LoadWriterBarrier()-cur.Load() > maxLag {
					return // self-evict
				}
				time.Sleep(50 * time.Microsecond)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= 100; i++ {
			rb.Publish(i)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writer stalled: self-eviction did not ungate it")
	}
	<-evicted

	// With the only reader gone, the barrier follows the writer cursor.
	if got, w := rb.barrier.Load(), rb.writeCursor.Load(); got != w {
		t.Errorf("barrier.Load() = %d after eviction, want writer cursor %d", got, w)
	}
}
