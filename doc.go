// Package spmc implements a cache-line-aware, single-writer
// multiple-reader ring buffer inspired by the LMAX Disruptor pattern.
//
// Key features:
//   - Lock-free reader lifecycle management via CAS bitmap
//   - Configurable wait strategies (Spin, Yield, Sleep, Hybrid)
//   - Batch publish primitives (Reserve/Commit, PublishBatch)
//   - Non-blocking variants (TryPublish, TryReserve) and a Remaining capacity signal
//   - Pipeline staging (Stage, SetGatingStage)
//   - False-sharing elimination through cache-line padding
//
// Quick start:
//
//	rbuf, _ := spmc.NewRingBuffer[int](ctx, 1024)
//	stage := rbuf.NewStage(nil) // gated by the writer cursor
//	rbuf.SetGatingStage(stage)  // writer waits for this stage's slowest reader
//	slotID, _ := stage.AddReader(func(ctx context.Context, rv spmc.ReadView[int], cur *atomic.Int64) {
//	    // read loop; see ExampleRingBuffer
//	})
//	rbuf.Publish(42)
package spmc
