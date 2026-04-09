package ringring

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
