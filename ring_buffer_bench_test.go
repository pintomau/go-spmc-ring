package ringring

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type object struct {
	x [CacheLineSize]byte
}

func produce(o *object) {
	o.x[0] = '0'
}

var obj *object // prevent optimization

func BenchmarkRingBuffer_Publish(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const c = 1 << 22
	capacity := int64(c)
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}

	// Add a reader that keeps up
	rb.barrier.AddReader(func(ctx context.Context, readView ReadView[object], readerCursor *atomic.Int64) {
		current := readerCursor.Load()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if writerBarrier := readView.LoadWriterBarrier(); current < writerBarrier {
					for seq := current + 1; seq <= writerBarrier; seq++ {
						obj = readView.Get(seq)
					}
					readerCursor.Store(writerBarrier)
					current = writerBarrier
				} else if writerBarrier := readView.LoadWriterBarrier(); current < writerBarrier {
					// try again
					for seq := current + 1; seq <= writerBarrier; seq++ {
						obj = readView.Get(seq)
					}
					readerCursor.Store(writerBarrier)
					current = writerBarrier
				} else {
					time.Sleep(50 * time.Microsecond)
				}
			}
		}
	})

	for b.Loop() {
		rb.PublishFunc(produce)
	}
}

// BenchmarkRingBuffer_TryPublish mirrors BenchmarkRingBuffer_Publish (same
// keep-up reader, same payload, TryPublishFunc instead of PublishFunc) so the
// two are directly comparable. The retry loop only spins when the ring is
// momentarily full.
func BenchmarkRingBuffer_TryPublish(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 22
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}

	rb.barrier.AddReader(keepUpReader)

	for b.Loop() {
		for !rb.TryPublishFunc(produce) {
		}
	}
}

func BenchmarkRingBuffer_Publish_NoReaders(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 22
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		rb.PublishFunc(produce)
	}
}

func BenchmarkRingBuffer_Publish_MultiReader(b *testing.B) {
	readerCounts := []int{1, 2, 4, 8, 16, 32, 64, 128}

	for _, readers := range readerCounts {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			const capacity = 1 << 22
			rb, err := NewRingBuffer[object](ctx, capacity)
			if err != nil {
				b.Fatal(err)
			}

			for range readers {
				if _, err := rb.barrier.AddReader(keepUpReader); err != nil {
					b.Fatal(err)
				}
			}

			for b.Loop() {
				rb.PublishFunc(produce)
			}
		})
	}
}

// Benchmark: compare Publish vs PublishFunc overhead
func BenchmarkRingBuffer_Publish_Direct(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 22
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}

	rb.barrier.AddReader(func(ctx context.Context, readView ReadView[object], readerCursor *atomic.Int64) {
		current := readerCursor.Load()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if writerBarrier := readView.LoadWriterBarrier(); current < writerBarrier {
					for seq := current + 1; seq <= writerBarrier; seq++ {
						obj = readView.Get(seq)
					}
					readerCursor.Store(writerBarrier)
					current = writerBarrier
				} else if writerBarrier := readView.LoadWriterBarrier(); current < writerBarrier {
					// try again
					for seq := current + 1; seq <= writerBarrier; seq++ {
						obj = readView.Get(seq)
					}
					readerCursor.Store(writerBarrier)
					current = writerBarrier
				} else {
					time.Sleep(50 * time.Microsecond)
				}
			}
		}
	})

	for b.Loop() {
		obj := object{}
		obj.x[0] = '0'
		rb.Publish(obj) // Direct publish instead of PublishFunc
	}
}

var objs []object // prevent optimization

func BenchmarkRingBuffer_Publish_BatchReader(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 22
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}

	// Batch reader: process contiguous segments
	rb.barrier.AddReader(func(ctx context.Context, readView ReadView[object], readerCursor *atomic.Int64) {
		var (
			current = readerCursor.Load()
			mask    = readView.GetMask()
		)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				w := readView.LoadWriterBarrier()

				if current < w {
					// Calculate segment boundaries
					start := (current + 1) & mask
					end := w & mask

					objs = readView.GetRange(start, end)

					readerCursor.Store(w)
					current = w
				} else {
					time.Sleep(50 * time.Microsecond)
				}
			}
		}
	})

	for b.Loop() {
		obj := object{}
		obj.x[0] = '0'
		rb.Publish(obj)
	}
}

func BenchmarkRingBuffer_Publish_IteratorReader(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 22
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}

	// Batch reader: process contiguous segments
	rb.barrier.AddReader(func(ctx context.Context, readView ReadView[object], readerCursor *atomic.Int64) {
		var (
			current = readerCursor.Load()
			mask    = readView.GetMask()
		)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				w := readView.LoadWriterBarrier()

				if current < w {
					// Calculate segment boundaries (like go-disruptor)
					start := (current + 1) & mask
					end := w & mask

					for obj = range readView.Iterate(start, end) {

					}

					readerCursor.Store(w)
					current = w
				} else {
					time.Sleep(50 * time.Microsecond)
				}
			}
		}
	})

	for b.Loop() {
		obj := object{}
		obj.x[0] = '0'
		rb.Publish(obj)
	}
}

var batchSizes = []int64{1, 10, 100, 1000}

// keepUpReader mirrors BenchmarkRingBuffer_Publish's reader: it drains whatever the
// writer has published so the batch writer stays on the hot path.
func keepUpReader(ctx context.Context, readView ReadView[object], readerCursor *atomic.Int64) {
	current := readerCursor.Load()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if w := readView.LoadWriterBarrier(); current < w {
				for seq := current + 1; seq <= w; seq++ {
					obj = readView.Get(seq)
				}
				readerCursor.Store(w)
				current = w
			} else {
				time.Sleep(50 * time.Microsecond)
			}
		}
	}
}

func BenchmarkRingBuffer_PublishBatch(b *testing.B) {
	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			const capacity = 1 << 22
			rb, err := NewRingBuffer[object](ctx, capacity)
			if err != nil {
				b.Fatal(err)
			}
			rb.barrier.AddReader(keepUpReader)

			payload := make([]object, size)
			for i := range payload {
				payload[i].x[0] = '0'
			}

			b.ResetTimer()
			for b.Loop() {
				rb.PublishBatch(payload)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(size), "ns/item")
		})
	}
}

func BenchmarkRingBuffer_PublishBatchFunc(b *testing.B) {
	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			const capacity = 1 << 22
			rb, err := NewRingBuffer[object](ctx, capacity)
			if err != nil {
				b.Fatal(err)
			}
			rb.barrier.AddReader(keepUpReader)

			fill := func(i int64, slot *object) { slot.x[0] = '0' }

			b.ResetTimer()
			for b.Loop() {
				rb.PublishBatchFunc(size, fill)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(size), "ns/item")
		})
	}
}

func BenchmarkRingBuffer_Reserve(b *testing.B) {
	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			const capacity = 1 << 22
			rb, err := NewRingBuffer[object](ctx, capacity)
			if err != nil {
				b.Fatal(err)
			}
			rb.barrier.AddReader(keepUpReader)

			b.ResetTimer()
			for b.Loop() {
				seg1, seg2, claim := rb.Reserve(size)
				for j := range seg1 {
					seg1[j].x[0] = '0'
				}
				for j := range seg2 {
					seg2[j].x[0] = '0'
				}
				rb.Commit(claim)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(size), "ns/item")
		})
	}
}
