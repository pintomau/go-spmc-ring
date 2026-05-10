package ringring

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// Data flow:
//
//	rapid.Check(t, 100 cases)
//	  ├─ draws: numReaders (1–50), burstLoops (1–10), joinDelays[numReaders] (0–100ms each)
//	  └─ rapid.SyncTest(rt, ...)           ← replaces synctest.Test(t, ...)
//	       ├─ creates BitmapReaderPool
//	       ├─ spawns writer goroutine (1ms tick)
//	       ├─ spawns numReaders actor goroutines
//	       │    each: waits joinDelays[i] ms independently, runs burstLoops read cycles
//	       ├─ runs 1000 checkpoint assertions (100ms virtual time each)
//	       │    → min ≤ writer, empty-state sentinel
//	       └─ any t.Fatal/t.Error → rapid captures → shrinks → reports
func TestBitmapReaderPool_Simulation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulation test in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		numReaders := rapid.IntRange(1, 50).Draw(rt, "numReaders")
		burstLoops := rapid.IntRange(1, 10).Draw(rt, "burstLoops")
		joinDelays := rapid.SliceOfN(rapid.IntRange(0, 100), numReaders, numReaders).Draw(rt, "joinDelayMs")

		rapid.SyncTest(rt, func(rt *rapid.T) {
			ctx, cancel := context.WithCancel(context.Background())

			writerCursor := &atomic.Int64{}
			pool := NewBitmapReaderPool[int](ctx, writerCursor)

			var wg sync.WaitGroup
			var activeReaders atomic.Int32

			// Simulate a writer advancing continuously.
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						time.Sleep(1 * time.Millisecond)
						writerCursor.Add(1)
					}
				}
			}()

			// Simulate random readers joining, working, and leaving.
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
							current := readerCursor.Load()

							for j := 0; j < burstLoops; j++ {
								select {
								case <-ctx.Done():
									return
								default:
									if target := readView.LoadWriterBarrier(); current < target {
										for seq := current + 1; seq <= target; seq++ {
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
							select {
							case <-done:
							case <-ctx.Done():
							}
							_ = pool.RemoveReader(slotId)
						}
					}
				}()
			}

			// Monitor invariants periodically.
			for range 1000 {
				time.Sleep(100 * time.Millisecond)

				minVal := pool.Load()
				wVal := writerCursor.Load()

				if minVal != maxInt64 && minVal > wVal {
					rt.Errorf("invariant violation: pool min (%d) > writer (%d)", minVal, wVal)
				}

				if activeReaders.Load() > 0 && minVal == maxInt64 {
					rt.Errorf("invariant violation: active readers exist but pool reports empty (maxInt64)")
				}
			}

			cancel()
			wg.Wait()
			pool.Shutdown()
		})
	})
}
