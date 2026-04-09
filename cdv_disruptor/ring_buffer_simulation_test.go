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

var rbSeedFlag = flag.Int64("rbSeed", 0, "Random seed for deterministic simulation")

func TestRingBuffer_DeterministicSimulation(t *testing.T) {
	seed := *rbSeedFlag
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	t.Logf("RingBuffer simulation running with seed: %d", seed)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		rbuf, err := NewRingBuffer[int](ctx, 64, WithWaitStrategy[int](WaitStrategySleep))
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		var activeReaders atomic.Int32
		var corruptionDetected atomic.Int32

		// Writer goroutine continually publishes increasing sequence numbers.
		wg.Go(func() {
			seq := int64(0)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					time.Sleep(1 * time.Millisecond)
					seq++
					rbuf.Publish(int(seq))
				}
			}
		})

		// Spawn reader actors that randomly join, process, and leave the ring buffer.
		for i := uint64(0); i < 50; i++ {
			readerSeed := i
			wg.Go(func() {
				r := rand.New(rand.NewPCG(uint64(seed), readerSeed))

				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Duration(r.IntN(100)) * time.Millisecond):
					}

					done := make(chan struct{})
					readerFn := func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
						activeReaders.Add(1)
						defer activeReaders.Add(-1)
						defer close(done)

						expected := readerCursor.Load() + 1
						loops := r.IntN(10) + 1

						for j := 0; j < loops; j++ {
							select {
							case <-ctx.Done():
								return
							default:
								writerPos := readView.LoadWriterBarrier()
								if expected <= writerPos {
									for seq := expected; seq <= writerPos; seq++ {
										if val := *readView.Get(seq); val != int(seq) {
											corruptionDetected.Store(1)
											return
										}
									}
									readerCursor.Store(writerPos)
									expected = writerPos + 1
								} else {
									time.Sleep(time.Millisecond)
								}
							}
						}
					}

					slotID, err := rbuf.barrier.AddReader(readerFn)
					if err == nil {
						select {
						case <-done:
						case <-ctx.Done():
						}
						_ = rbuf.barrier.RemoveReader(slotID)
					}
				}
			})
		}

		// Periodically assert invariants under simulated time.
		for range 1000 {
			time.Sleep(100 * time.Millisecond)

			if corruptionDetected.Load() != 0 {
				t.Fatalf("payload corruption detected (seed=%d)", seed)
			}

			minVal := rbuf.barrier.Load()
			writerVal := rbuf.writeCursor.Load()

			if minVal != maxInt64 && minVal > writerVal {
				t.Errorf("invariant violation: min (%d) > writer (%d) seed=%d", minVal, writerVal, seed)
			}

			if activeReaders.Load() > 0 && minVal == maxInt64 {
				t.Errorf("invariant violation: active readers exist but min=maxInt64 seed=%d", seed)
			}
		}

		cancel()
		wg.Wait()
		rbuf.barrier.Shutdown()
	})
}
