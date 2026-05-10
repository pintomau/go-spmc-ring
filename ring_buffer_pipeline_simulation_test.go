package ringring

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// TestPipeline_Simulation is a property-based simulation that exercises
// multi-stage pipelines with randomly drawn depths, reader counts, and join
// timings. rapid.SyncTest advances virtual time so the 500-tick checkpoint
// loop runs without real wall-clock cost.
//
// Invariants checked at each tick:
//  1. Ordering: writeCursor ≥ stage[0].min ≥ stage[1].min ≥ … ≥ leaf.min
//  2. Writer bound: writeCursor − leaf.min ≤ bufferCapacity
//  3. Value integrity: each reader verifies rv.Get(seq) == seq (no corruption)
//
// Run pipeline simulations without the ring-buffer simulation suite:
//
//	go test -run=TestPipeline_Simulation -timeout 300s ./...
func TestPipeline_Simulation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulation test in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		numStages := rapid.IntRange(1, 3).Draw(rt, "numStages")
		readersPerStage := rapid.SliceOfN(rapid.IntRange(1, 3), numStages, numStages).Draw(rt, "readersPerStage")
		bufferCapacity := rapid.SampledFrom([]int64{8, 16, 64}).Draw(rt, "bufferCapacity")

		// Per-reader join delays drawn outside SyncTest so rapid can shrink them.
		joinDelayMs := make([][]int, numStages)
		for i := range numStages {
			joinDelayMs[i] = rapid.SliceOfN(
				rapid.IntRange(0, 50), readersPerStage[i], readersPerStage[i],
			).Draw(rt, fmt.Sprintf("joinDelayMs_stage%d", i))
		}

		rapid.SyncTest(rt, func(rt *rapid.T) {
			ctx, cancel := context.WithCancel(context.Background())

			rb, err := NewRingBuffer[int](ctx, bufferCapacity, WithWaitStrategy[int](WaitStrategySleep))
			if err != nil {
				rt.Fatal(err)
			}

			stages := make([]*Stage[int], numStages)
			stages[0] = rb.NewStage(nil)
			for i := 1; i < numStages; i++ {
				stages[i] = rb.NewStage(stages[i-1].Barrier())
			}
			rb.SetGatingStage(stages[numStages-1])

			var wg sync.WaitGroup
			var corruptionDetected atomic.Int32

			// Spawn one goroutine per reader. Each waits its join delay, adds
			// itself to the stage, verifies every value it reads, then removes
			// itself on exit.
			for i, stage := range stages {
				for j := range readersPerStage[i] {
					delayMs := joinDelayMs[i][j]
					wg.Add(1)
					go func() {
						defer wg.Done()
						select {
						case <-ctx.Done():
							return
						case <-time.After(time.Duration(delayMs) * time.Millisecond):
						}

						done := make(chan struct{})
						slotID, err := stage.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
							defer close(done)
							expected := cur.Load() + 1
							for {
								select {
								case <-ctx.Done():
									return
								default:
									w := rv.LoadWriterBarrier()
									if expected <= w {
										for seq := expected; seq <= w; seq++ {
											if val := *rv.Get(seq); val != int(seq) {
												corruptionDetected.Store(1)
												return
											}
										}
										cur.Store(w)
										expected = w + 1
									} else {
										time.Sleep(time.Millisecond)
									}
								}
							}
						})
						if err == nil {
							select {
							case <-done:
							case <-ctx.Done():
							}
							_ = stage.RemoveReader(slotID)
						}
					}()
				}
			}

			// Writer publishes seq at sequence seq so readers can verify values.
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
						rb.Publish(int(seq))
					}
				}
			}()

			// Checkpoint loop: 500 ticks × 100 ms virtual = 50 s simulated time.
			mins := make([]int64, numStages)
			for range 500 {
				time.Sleep(100 * time.Millisecond)

				if corruptionDetected.Load() != 0 {
					rt.Fatal("data corruption detected in pipeline reader")
				}

				// Sample stage minimums from leaf to root. Stage[i] can only
				// advance after stage[i-1] has advanced, so reading downstream
				// stages first guarantees that the subsequent upstream reads
				// reflect a value ≥ the already-sampled downstream minimum.
				for i := numStages - 1; i >= 0; i-- {
					mins[i] = stages[i].Barrier().Load()
				}
				writerPos := rb.writeCursor.Load()

				// Invariant 1: writeCursor ≥ stage[0].min ≥ … ≥ stage[N-1].min
				prevMin := writerPos
				for i, stageMin := range mins {
					if stageMin > prevMin {
						rt.Errorf("ordering violation: stage %d min %d > upstream min %d (writeCursor=%d)",
							i, stageMin, prevMin, writerPos)
					}
					prevMin = stageMin
				}

				// Invariant 2: writer has not overrun the leaf stage.
				if writerPos-mins[numStages-1] > bufferCapacity {
					rt.Errorf("writer overran leaf: writeCursor=%d leafMin=%d bufferCapacity=%d delta=%d",
						writerPos, mins[numStages-1], bufferCapacity, writerPos-mins[numStages-1])
				}
			}

			cancel()
			wg.Wait()
			for _, stage := range stages {
				stage.Shutdown()
			}
			rb.barrier.Shutdown()
		})
	})
}
