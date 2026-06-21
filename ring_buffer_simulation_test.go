package ringring

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

type batchWriteMethod string

const (
	batchWriteMethodPublishBatch     batchWriteMethod = "publish_batch"
	batchWriteMethodPublishBatchFunc batchWriteMethod = "publish_batch_func"
	batchWriteMethodReserveCommit    batchWriteMethod = "reserve_commit"
	batchWriteMethodTryPublish       batchWriteMethod = "try_publish"
	batchWriteMethodTryReserveCommit batchWriteMethod = "try_reserve_commit"
)

// Data flow:
//
//	rapid.Check(t, 100 cases)
//	  ├─ draws: numReaders (1–50), burstLoops (1–10), joinDelays[numReaders] (0–100ms each)
//	  └─ rapid.SyncTest(rt, ...)           ← replaces synctest.Test(t, ...)
//	       ├─ creates RingBuffer
//	       ├─ spawns writer goroutine (1ms tick, unchanged)
//	       ├─ spawns numReaders actor goroutines
//	       │    each: waits joinDelays[i] ms independently, runs burstLoops read cycles
//	       ├─ runs 1000 checkpoint assertions (100ms virtual time each)
//	       │    → corruption, min ≤ writer, empty-state sentinel
//	       └─ any t.Fatal/t.Error → rapid captures → shrinks → reports
func TestRingBuffer_Simulation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulation test in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		numReaders := rapid.IntRange(1, 50).Draw(rt, "numReaders")
		burstLoops := rapid.IntRange(1, 10).Draw(rt, "burstLoops")
		joinDelays := rapid.SliceOfN(rapid.IntRange(0, 100), numReaders, numReaders).Draw(rt, "joinDelayMs")

		rapid.SyncTest(rt, func(rt *rapid.T) {
			ctx, cancel := context.WithCancel(context.Background())

			rbuf, err := NewRingBuffer[int](ctx, 64, WithWaitStrategy[int](WaitStrategySleep))
			if err != nil {
				rt.Fatal(err)
			}

			var wg sync.WaitGroup
			var activeReaders atomic.Int32
			var corruptionDetected atomic.Int32

			// Writer goroutine continually publishes increasing sequence numbers.
			wg.Add(1)
			go func() {
				defer wg.Done()
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
			}()

			// Spawn reader actors that randomly join, process, and leave the ring buffer.
			for i := 0; i < numReaders; i++ {
				delay := joinDelays[i]
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-ctx.Done():
							return
						case <-time.After(time.Duration(delay) * time.Millisecond):
						}

						done := make(chan struct{})
						readerFn := func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
							activeReaders.Add(1)
							defer activeReaders.Add(-1)
							defer close(done)

							expected := readerCursor.Load() + 1
							for j := 0; j < burstLoops; j++ {
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
				}()
			}

			// Periodically assert invariants under simulated time.
			for range 1000 {
				time.Sleep(100 * time.Millisecond)

				if corruptionDetected.Load() != 0 {
					rt.Fatalf("payload corruption detected")
				}

				minVal := rbuf.barrier.Load()
				writerVal := rbuf.writeCursor.Load()

				if minVal != maxInt64 && minVal > writerVal {
					rt.Errorf("invariant violation: min (%d) > writer (%d)", minVal, writerVal)
				}

				if activeReaders.Load() > 0 && minVal == maxInt64 {
					rt.Errorf("invariant violation: active readers exist but min=maxInt64")
				}
			}

			cancel()
			wg.Wait()
			rbuf.barrier.Shutdown()
		})
	})
}

func TestRingBuffer_BatchSimulation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulation test in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		numReaders := rapid.IntRange(1, 8).Draw(rt, "numReaders")
		readerIdleDelays := rapid.SliceOfN(rapid.IntRange(0, 3), numReaders, numReaders).Draw(rt, "readerIdleDelayMs")
		writerPatternLen := rapid.IntRange(1, 16).Draw(rt, "writerPatternLen")
		writerMethods := rapid.SliceOfN(
			rapid.SampledFrom([]batchWriteMethod{
				batchWriteMethodPublishBatch,
				batchWriteMethodPublishBatchFunc,
				batchWriteMethodReserveCommit,
				batchWriteMethodTryPublish,
				batchWriteMethodTryReserveCommit,
			}),
			writerPatternLen,
			writerPatternLen,
		).Draw(rt, "writerMethods")
		batchSizes := rapid.SliceOfN(rapid.IntRange(1, 15), writerPatternLen, writerPatternLen).Draw(rt, "batchSizes")

		rapid.SyncTest(rt, func(rt *rapid.T) {
			ctx, cancel := context.WithCancel(context.Background())

			rbuf, err := NewRingBuffer[int](ctx, 128, WithWaitStrategy[int](WaitStrategySleep))
			if err != nil {
				rt.Fatal(err)
			}

			var wg sync.WaitGroup
			var activeReaders atomic.Int32
			var corruptionDetected atomic.Int32
			var tryFalseViolation atomic.Int32
			slotIDs := make([]int, 0, numReaders)

			defer func() {
				cancel()
				for _, slotID := range slotIDs {
					_ = rbuf.barrier.RemoveReader(slotID)
				}
				wg.Wait()
				rbuf.barrier.Shutdown()
			}()

			// Writer cycles through the batch APIs so the same simulation exercises
			// PublishBatch, PublishBatchFunc, and manual Reserve/Commit paths.
			wg.Add(1)
			go func() {
				defer wg.Done()
				seq := int64(0)
				step := 0
				for {
					select {
					case <-ctx.Done():
						return
					default:
						time.Sleep(1 * time.Millisecond)

						batchSize := int64(batchSizes[step%len(batchSizes)])
						firstSeq := seq + 1

						switch writerMethods[step%len(writerMethods)] {
						case batchWriteMethodPublishBatch:
							payload := make([]int, int(batchSize))
							for i := range payload {
								payload[i] = int(firstSeq + int64(i))
							}
							rbuf.PublishBatch(payload)
						case batchWriteMethodPublishBatchFunc:
							rbuf.PublishBatchFunc(batchSize, func(i int64, slot *int) {
								*slot = int(firstSeq + i)
							})
						case batchWriteMethodReserveCommit:
							seg1, seg2, claim := rbuf.Reserve(batchSize)
							next := firstSeq
							for i := range seg1 {
								seg1[i] = int(next)
								next++
							}
							for i := range seg2 {
								seg2[i] = int(next)
								next++
							}
							rbuf.Commit(claim)
						case batchWriteMethodTryPublish:
							for i := int64(0); i < batchSize; {
								if rbuf.TryPublish(int(firstSeq + i)) {
									i++
									continue
								}
								// TryPublish just refreshed cachedSlowestReader, and this
								// goroutine is the only writer, so a false return must
								// mean the ring is genuinely full right now.
								if seq+i+1 < rbuf.cachedSlowestReader+rbuf.bufferSize {
									tryFalseViolation.Store(1)
									return
								}
								time.Sleep(time.Millisecond)
							}
						case batchWriteMethodTryReserveCommit:
							for {
								seg1, seg2, claim, ok := rbuf.TryReserve(batchSize)
								if ok {
									next := firstSeq
									for i := range seg1 {
										seg1[i] = int(next)
										next++
									}
									for i := range seg2 {
										seg2[i] = int(next)
										next++
									}
									rbuf.Commit(claim)
									break
								}
								if seq+batchSize < rbuf.cachedSlowestReader+rbuf.bufferSize {
									tryFalseViolation.Store(1)
									return
								}
								time.Sleep(time.Millisecond)
							}
						default:
							panic("unknown batch writer method")
						}

						seq += batchSize
						step++
					}
				}
			}()

			for i := 0; i < numReaders; i++ {
				idleDelay := time.Duration(readerIdleDelays[i]+1) * time.Millisecond
				slotID, err := rbuf.barrier.AddReader(func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
					activeReaders.Add(1)
					defer activeReaders.Add(-1)

					expected := readerCursor.Load() + 1
					for {
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
								time.Sleep(idleDelay)
							}
						}
					}
				})
				if err != nil {
					rt.Fatal(err)
				}
				slotIDs = append(slotIDs, slotID)
			}

			// Periodically assert invariants under simulated time.
			for range 250 {
				time.Sleep(20 * time.Millisecond)

				if corruptionDetected.Load() != 0 {
					rt.Fatalf("batch payload corruption detected")
				}

				if tryFalseViolation.Load() != 0 {
					rt.Fatalf("try variant returned false while the ring had free capacity")
				}

				minVal := rbuf.barrier.Load()
				writerVal := rbuf.writeCursor.Load()

				if minVal != maxInt64 && minVal > writerVal {
					rt.Errorf("invariant violation: min (%d) > writer (%d)", minVal, writerVal)
				}

				if activeReaders.Load() > 0 && minVal == maxInt64 {
					rt.Errorf("invariant violation: active readers exist but min=maxInt64")
				}
			}
		})
	})
}
