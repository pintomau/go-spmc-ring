package cdv_disruptor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type object struct {
	x [128]byte
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
					// Calculate segment boundaries (like go-disruptor)
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
