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
	s.RemoveReader(slotID)
	s.Shutdown()

	// Output:
	// Read: 10
	// Read: 20
	// Read: 30
}
