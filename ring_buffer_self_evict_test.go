package ringring

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

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
