package spmc_test

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pintomau/go-spmc-ring"
)

// This example demonstrates a basic publish-and-read cycle using a pipeline stage.
// A single reader goroutine processes values published by the main goroutine.
func ExampleRingBuffer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := spmc.NewRingBuffer[int](ctx, 64)
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
	slotID, err := s.AddReader(func(ctx context.Context, rv spmc.ReadView[int], cur *atomic.Int64) {
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

// This example demonstrates batch I/O: Reserve/Commit on the write side and
// GetSegments on the read side. Reserve allocates contiguous slots (up to two
// segments when wrapping). After filling them, a single Commit makes the whole
// batch visible atomically. The reader then uses GetSegments to obtain
// zero-copy views of the available range and processes all events at once.
func ExampleRingBuffer_batch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := spmc.NewRingBuffer[int](ctx, 8)
	if err != nil {
		fmt.Println("error creating ring buffer:", err)
		return
	}

	s := rb.NewStage(nil)
	rb.SetGatingStage(s)

	stall := make(chan struct{}, 1)
	// Channel to collect batch results from the reader.
	batchSums := make(chan int, 10)

	// Register a reader that processes events in batches using GetSegments.
	slotID, err := s.AddReader(func(ctx context.Context, rv spmc.ReadView[int], cur *atomic.Int64) {
		<-stall
		expected := cur.Load() + 1
		rv.Get(expected)
		expected++
		cur.Store(expected)
		<-stall
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if w := rv.LoadWriterBarrier(); expected <= w {
					// GetSegments returns zero-copy views of the available
					// range. seg2 is non-nil only when the range wraps the
					// ring end.
					seg1, seg2 := rv.GetSegments(expected, w)

					// Process all events in the batch.
					sum := 0
					for i := range seg1 {
						sum += seg1[i]
					}
					for i := range seg2 {
						sum += seg2[i]
					}
					batchSums <- sum

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

	// force a wrapping on reserve
	rb.Publish(1000)
	stall <- struct{}{}

	// Batch write: Reserve allocates 7 slots, returning up to two segments
	seg1, seg2, claim := rb.Reserve(7)

	// Fill both segments. seg1 contains slots up to the ring end, seg2
	val := 100
	for i := range seg1 {
		seg1[i] = val
		val++
	}
	for i := range seg2 {
		seg2[i] = val
		val++
	}

	// Commit makes the entire batch visible atomically.
	rb.Commit(claim)
	stall <- struct{}{}

	// Collect the batch sum from the reader.
	fmt.Println("Batch sum:", <-batchSums)

	// Clean up.
	_ = s.RemoveReader(slotID)
	s.Shutdown()
	close(stall)

	// Output:
	// Batch sum: 721
}

// This example shows a reader that evicts itself when it falls too far
// behind, instead of stalling the writer forever. Returning from a ReaderFunc
// is a first-class way to leave the pool: the slot is deactivated and freed,
// and the writer is ungated. The reader picks its own exit point, so it is
// never mid-read when the writer reclaims its slots.
func ExampleRingBuffer_selfEvictingReader() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := spmc.NewRingBuffer[int](ctx, 8)
	if err != nil {
		fmt.Println("error creating ring buffer:", err)
		return
	}

	s := rb.NewStage(nil)
	rb.SetGatingStage(s)

	evicted := make(chan struct{})
	_, err = s.AddReader(func(ctx context.Context, rv spmc.ReadView[int], cur *atomic.Int64) {
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
