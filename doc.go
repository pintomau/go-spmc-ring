// Package ringring implements a cache-line-aware, single-writer
// multiple-reader ring buffer inspired by the LMAX Disruptor pattern.
//
// Key features:
//   - Lock-free reader lifecycle management via CAS bitmap
//   - Configurable wait strategies (Spin, Yield, Sleep, Hybrid)
//   - Batch publish primitives (Reserve/Commit, PublishBatch)
//   - Pipeline staging (Stage, SetGatingStage)
//   - False-sharing elimination through cache-line padding
//
// Quick start:
//
//	rbuf, _ := ringring.NewRingBuffer[int](ctx, 1024)
//	slotID, _ := rbuf.Barrier().AddReader(func(ctx context.Context, rv ringring.ReadView[int], cur *atomic.Int64) {
//	    // read loop
//	})
//	rbuf.Publish(42)
package ringring
