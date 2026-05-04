package cdv_disruptor

import (
	"context"
	"flag"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

var seedFlag = flag.Int64("seed", 0, "Random seed for deterministic simulation")

func TestBitmapReaderPool_DeterministicSimulation(t *testing.T) {
	seed := *seedFlag
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	t.Logf("Simulation running with seed: %d", seed)

	// virtual time bubble.
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		writerCursor := &atomic.Int64{}
		pool := NewBitmapReaderPool[int](ctx, writerCursor)

		var wg sync.WaitGroup
		// 1. Simulate a writer advancing continuously
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					time.Sleep(1 * time.Millisecond) // Virtual time
					writerCursor.Add(1)
				}
			}
		})

		// 2. Simulate random readers joining, working, and leaving
		var activeReaders atomic.Int32
		// Spawn 50 "actors" that will randomly join/leave the pool
		for i := uint64(0); i < 50; i++ {
			wg.Go(func() {
				// Deterministic RNG per goroutine
				r := rand.New(rand.NewPCG(uint64(seed), i))

				for {
					// Randomly decide when to join
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Duration(r.IntN(100)) * time.Millisecond):
					}

					// Define a reader that reads for a bit then exits
					done := make(chan struct{})
					readerFn := func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
						activeReaders.Add(1)
						defer activeReaders.Add(-1)
						current := readerCursor.Load()

						// Simulate processing work
						loops := r.IntN(10) + 1
						for j := 0; j < loops; j++ {
							select {
							case <-ctx.Done():
								return
							default:
								if target := readView.LoadWriterBarrier(); current < target {
									for seq := current + 1; seq <= target; seq++ {
										// Simulate work
										time.Sleep(10 * time.Millisecond)
									}
									readerCursor.Store(target)
									current = target
								} else {
									time.Sleep(time.Millisecond)
								}
							}
						}
						close(done)
					}

					slotId, err := pool.AddReader(readerFn)
					if err == nil {
						// Wait for reader to finish its work naturally
						select {
						case <-done:
						case <-ctx.Done():
						}
						_ = pool.RemoveReader(slotId)
					}
				}
			})
		}

		// 3. Monitor Invariants
		// We check the state of the pool periodically.
		// In synctest, time.Sleep yields to other goroutines until the duration passes.
		for range 1000 {
			time.Sleep(100 * time.Millisecond)

			minVal := pool.Load()
			wVal := writerCursor.Load()

			// Invariant 1: The pool minimum should never exceed the writer
			// (Readers can't read future data)
			if minVal != maxInt64 && minVal > wVal {
				t.Errorf("Invariant violation: pool min (%d) > writer (%d)", minVal, wVal)
			}

			// Invariant 2: If there are active readers, min should be <= writer
			// If there are NO readers, min should be maxInt64
			if activeReaders.Load() > 0 && minVal == maxInt64 {
				t.Errorf("Invariant violation: active readers exist but pool reports empty (maxInt64)")
			}
		}

		// Signal shutdown
		cancel()
		// Wait for all actors to finish
		wg.Wait()

		// Wait for pool internal goroutines to finish
		pool.Shutdown()
	})
}
