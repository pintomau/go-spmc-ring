package ringring_test

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pintomau/ringring"
)

// This example demonstrates a basic publish-and-read cycle using a pipeline stage.
// A single reader goroutine processes values published by the main goroutine.
func ExampleRingBuffer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := ringring.NewRingBuffer[int](ctx, 64)
	if err != nil {
		fmt.Println("error creating ring buffer:", err)
		return
	}

	// Create a stage and set it as the gating barrier.
	s := rb.NewStage(nil)
	rb.SetGatingStage(s)

	// Channel to collect read values so the example can print them.
	read := make(chan int, 10)

	// Register a reader that collects published values.
	slotID, err := s.AddReader(func(ctx context.Context, rv ringring.ReadView[int], cur *atomic.Int64) {
		expected := cur.Load() + 1
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if w := rv.LoadWriterBarrier(); expected <= w {
					for seq := expected; seq <= w; seq++ {
						read <- *rv.Get(seq)
					}
					cur.Store(w)
					expected = w + 1
				}
			}
		}
	})
	if err != nil {
		fmt.Println("error adding reader:", err)
		return
	}

	// Publish some values.
	for _, v := range []int{10, 20, 30} {
		rb.Publish(v)
	}

	// Collect the values the reader picked up.
	for i := 0; i < 3; i++ {
		fmt.Println("Read:", <-read)
	}

	// Clean up.
	_ = s.RemoveReader(slotID)
	s.Shutdown()

	// Output:
	// Read: 10
	// Read: 20
	// Read: 30
}

// This example shows a reader that evicts itself when it falls too far
// behind, instead of stalling the writer forever. Returning from a ReaderFunc
// is a first-class way to leave the pool: the slot is deactivated and freed,
// and the writer is ungated. The reader picks its own exit point, so it is
// never mid-read when the writer reclaims its slots.
func ExampleRingBuffer_selfEvictingReader() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := ringring.NewRingBuffer[int](ctx, 8)
	if err != nil {
		fmt.Println("error creating ring buffer:", err)
		return
	}

	s := rb.NewStage(nil)
	rb.SetGatingStage(s)

	evicted := make(chan struct{})
	_, err = s.AddReader(func(ctx context.Context, rv ringring.ReadView[int], cur *atomic.Int64) {
		defer close(evicted)
		const maxLag = 4
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if rv.LoadWriterBarrier()-cur.Load() > maxLag {
					return // self-evict: slot is freed, writer is ungated
				}
			}
		}
	})
	if err != nil {
		fmt.Println("error adding reader:", err)
		return
	}

	// 100 events exceed the capacity-8 ring many times over. The blocking
	// Publish calls can only complete because the laggard removes itself.
	for i := 1; i <= 100; i++ {
		rb.Publish(i)
	}
	<-evicted
	fmt.Println("published 100 events past a stalled reader")

	s.Shutdown()

	// Output:
	// published 100 events past a stalled reader
}
