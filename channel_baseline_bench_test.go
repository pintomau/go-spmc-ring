package spmc

import (
	"fmt"
	"sync"
	"testing"
)

// Standalone baseline: plain Go channels vs the ring buffer's broadcast fan-out.
//
// This file is intentionally separate from the ring-buffer benchmark suite. It
// does not touch RingBuffer, BitmapReaderPool, or any pipeline machinery; it
// exists only to give BenchmarkRingBuffer_Publish_MultiReader a like-for-like
// reference point built from stdlib primitives. It is not part of bench:core.
//
// Semantics match the ring's multi-reader path: every published event is
// delivered to *every* reader (broadcast), so the channel equivalent is one
// buffered channel per reader rather than a single shared (work-stealing)
// channel. Each send is a separate runtime channel operation, which is exactly
// the cost the ring buffer avoids by letting all readers observe one cursor.
//
// Compare with benchstat:
//
//	go test -run=^$ -bench='BenchmarkRingBuffer_Publish_MultiReader$' -count=10 . | tee out.txt
//	go test -run=^$ -bench='BenchmarkChannelFanout_MultiReader$'      -count=10 . | tee -a out.txt
//	benchstat out.txt

// chanFanoutCapacity is the per-reader channel buffer. The ring buffer runs at
// 1<<22 so the writer effectively never blocks; a per-reader channel that large
// would cost capacity*sizeof(object)*readers of memory (gigabytes at 128
// readers), so we use a smaller buffer here. With readers that keep up this is
// deep enough that the writer rarely blocks on a full channel, isolating the
// per-send channel-op cost rather than backpressure.
const chanFanoutCapacity = 1 << 12

func BenchmarkChannelFanout_MultiReader(b *testing.B) {
	readerCounts := []int{1, 2, 4, 8, 16, 32, 64, 128}

	for _, readers := range readerCounts {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			channels := make([]chan object, readers)
			for i := range channels {
				channels[i] = make(chan object, chanFanoutCapacity)
			}

			// Each reader drains its own channel into a private sink, so there
			// are no shared writes during the timed region. Sinks are retained
			// past the loop to prevent the compiler from eliding the reads.
			sinks := make([]object, readers)
			var wg sync.WaitGroup
			wg.Add(readers)
			for i := range channels {
				go func(id int, ch <-chan object) {
					defer wg.Done()
					var sink object
					for o := range ch {
						sink = o
					}
					sinks[id] = sink
				}(i, channels[i])
			}

			for b.Loop() {
				var o object
				produce(&o)
				for _, ch := range channels {
					ch <- o
				}
			}

			// Shutdown is outside the timed loop: close every channel, let the
			// readers drain and exit, then keep the sinks alive.
			for _, ch := range channels {
				close(ch)
			}
			wg.Wait()
			obj = &sinks[0]
		})
	}
}

// chanBatchFanoutItems is the per-reader channel buffer expressed in items, so
// buffering depth is comparable across batch sizes (more batches in flight when
// each batch is small). It matches the non-batched baseline's per-reader depth.
const chanBatchFanoutItems = 1 << 12

// BenchmarkChannelBatchFanout_MultiReader is the batched analog of
// BenchmarkChannelFanout_MultiReader: the writer accumulates `size` events into
// a slice and sends the slice as a single channel element (chan []object), so
// each reader pays one channel op per batch instead of per item. This is the
// stdlib equivalent of the ring's PublishBatch (one cursor store per batch).
//
// Reader count is fixed at 8 (a representative fan-out within hardware threads);
// the sweep is over batch size, mirroring BenchmarkRingBuffer_PublishBatch.
// Because PublishBatch is single-reader, compare *shapes* here (per-item cost
// collapsing with batch size, allocations persisting) rather than absolute ns.
//
// b.ReportAllocs surfaces the cost the ring never pays: broadcast means every
// reader shares the slice, so its backing array cannot be recycled until the
// slowest reader is done. Allocating a fresh batch per publish is the simplest
// correct choice; recycling instead would mean rebuilding the ring's
// slowest-reader cursor tracking on top of channels.
func BenchmarkChannelBatchFanout_MultiReader(b *testing.B) {
	const readers = 8

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			bufBatches := chanBatchFanoutItems / int(size)
			if bufBatches < 2 {
				bufBatches = 2
			}

			channels := make([]chan []object, readers)
			for i := range channels {
				channels[i] = make(chan []object, bufBatches)
			}

			sinks := make([]object, readers)
			var wg sync.WaitGroup
			wg.Add(readers)
			for i := range channels {
				go func(id int, ch <-chan []object) {
					defer wg.Done()
					var sink object
					for batch := range ch { // batch read: one recv drains `size` items
						for j := range batch {
							sink = batch[j]
						}
					}
					sinks[id] = sink
				}(i, channels[i])
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				batch := make([]object, size)
				for j := range batch {
					produce(&batch[j])
				}
				for _, ch := range channels {
					ch <- batch // batch insert: one channel op per batch
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(size), "ns/item")

			for _, ch := range channels {
				close(ch)
			}
			wg.Wait()
			obj = &sinks[0]
		})
	}
}
