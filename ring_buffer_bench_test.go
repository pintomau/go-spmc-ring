package ringring

import (
	"context"
	"fmt"
	"runtime"
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

// The DirectGap family decomposes the Publish_Direct vs Publish gap (see the
// PERFORMANCE.md section "The Publish_Direct gap, decomposed"): payload size
// is free (line ownership dominates), by-value costs ~7%, and the rest of
// Publish_Direct's gap is the per-event composite-literal construction, whose
// mixed-size stores defeat store-to-load forwarding on the argument copy.

// Publish with a value prepared once outside the loop: isolates the by-value
// API cost (arg copy + slot copy) from per-iteration construction.
func BenchmarkRingBuffer_DirectGap_Publish_Prepared(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 22
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}
	rb.barrier.AddReader(keepUpReader)

	var payload object
	payload.x[0] = '0'
	for b.Loop() {
		rb.Publish(payload)
	}
}

// PublishFunc writing the full 64 B payload: same memory traffic into the
// ring as Publish, through the callback API.
func BenchmarkRingBuffer_DirectGap_PublishFunc_FullFill(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 22
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}
	rb.barrier.AddReader(keepUpReader)

	var payload object
	payload.x[0] = '0'
	fill := func(o *object) { *o = payload }
	for b.Loop() {
		rb.PublishFunc(fill)
	}
}

// The Publish_Direct shape (zero + construct + publish by value) against
// keepUpReader so all DirectGap variants share a reader.
func BenchmarkRingBuffer_DirectGap_Publish_Constructed(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 22
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}
	rb.barrier.AddReader(keepUpReader)

	for b.Loop() {
		obj := object{}
		obj.x[0] = '0'
		rb.Publish(obj)
	}
}

// PublishFunc writing 1 byte (the standing Publish bench workload), with
// keepUpReader, as the in-family floor reference.
func BenchmarkRingBuffer_DirectGap_PublishFunc_OneByte(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 22
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}
	rb.barrier.AddReader(keepUpReader)

	for b.Loop() {
		rb.PublishFunc(produce)
	}
}

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
		current := readerCursor.Load()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				w := readView.LoadWriterBarrier()

				if current < w {
					seg1, seg2 := readView.GetSegments(current+1, w)
					for i := range seg1 {
						obj = &seg1[i]
					}
					for i := range seg2 {
						obj = &seg2[i]
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

func BenchmarkRingBuffer_Publish_IteratorReader(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 22
	rb, err := NewRingBuffer[object](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}

	// Iterator reader: range over each available batch
	rb.barrier.AddReader(func(ctx context.Context, readView ReadView[object], readerCursor *atomic.Int64) {
		current := readerCursor.Load()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				w := readView.LoadWriterBarrier()

				if current < w {
					for obj = range readView.Iterate(current+1, w) {
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

// Batch-publish benchmarks. NOTE: these do NOT all do equal per-slot work, so
// compare per-item ns across them with care. PublishBatch copies whole objects
// (copy()/memmove); PublishBatchFunc and Reserve below write only one byte per
// slot. On arm64 a sub-line write into a cold slot pays a write-allocate (RFO)
// the full-line copy avoids, so the 1-byte variants look artificially slow. The
// *_Fill variants do the equal-work (full-slot) comparison; see PERFORMANCE.md
// "Batch scaling".
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

// Reserve_Fill is BenchmarkRingBuffer_Reserve with a full-slot fill
// (seg[j] = payload, a whole-object copy per slot) instead of the 1-byte fill.
// Paired with PublishBatchFunc_Fill it equalizes per-slot memory traffic across
// the in-place APIs, so the per-item number reflects API/access-pattern cost
// rather than fill size. Contrast with PublishBatch, which fills via a single
// bulk copy(); see PERFORMANCE.md "Batch scaling".
func BenchmarkRingBuffer_Reserve_Fill(b *testing.B) {
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

			var payload object
			payload.x[0] = '0'

			b.ResetTimer()
			for b.Loop() {
				seg1, seg2, claim := rb.Reserve(size)
				for j := range seg1 {
					seg1[j] = payload
				}
				for j := range seg2 {
					seg2[j] = payload
				}
				rb.Commit(claim)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(size), "ns/item")
		})
	}
}

// PublishBatchFunc_Fill is BenchmarkRingBuffer_PublishBatchFunc with a full-slot
// fill (*slot = payload) instead of the 1-byte fill. See Reserve_Fill.
func BenchmarkRingBuffer_PublishBatchFunc_Fill(b *testing.B) {
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

			var payload object
			payload.x[0] = '0'
			fill := func(i int64, slot *object) { *slot = payload }

			b.ResetTimer()
			for b.Loop() {
				rb.PublishBatchFunc(size, fill)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(size), "ns/item")
		})
	}
}

// Read-path access benchmarks. The ReadView is constructed directly over a
// pre-filled buffer (no ring, no goroutines) so the numbers isolate pure
// traversal cost per access path. Work per event: XOR one byte into a sink.
// The wrapped variants drain a range that straddles the ring end, the worst
// case for batch reads; GetSegments stays zero-copy there.

const readPathCapacity = 1 << 13

var readPathBatches = []int64{10, 256, 4096}

var byteSink byte

func newReadPathView() ReadView[object] {
	buf := make([]object, readPathCapacity)
	for i := range buf {
		buf[i].x[0] = byte(i)
	}
	return ReadView[object]{buffer: buf, mask: readPathCapacity - 1}
}

// readPathRange picks absolute start/end so the range straddles the ring end.
func readPathRange(n int64) (start, end int64) {
	start = readPathCapacity - n/2 - 1
	return start, start + n - 1
}

func benchReadPath(b *testing.B, drain func(rv ReadView[object], start, end int64, acc byte) byte) {
	for _, n := range readPathBatches {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			rv := newReadPathView()
			start, end := readPathRange(n)
			b.ReportAllocs()
			var acc byte
			for b.Loop() {
				acc = drain(rv, start, end, acc)
			}
			byteSink = acc
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/item")
		})
	}
}

func BenchmarkReadView_GetSegments(b *testing.B) {
	benchReadPath(b, func(rv ReadView[object], start, end int64, acc byte) byte {
		seg1, seg2 := rv.GetSegments(start, end)
		for i := range seg1 {
			acc ^= seg1[i].x[0]
		}
		for i := range seg2 {
			acc ^= seg2[i].x[0]
		}
		return acc
	})
}

func BenchmarkReadView_GetSegments_NoWrap(b *testing.B) {
	benchReadPath(b, func(rv ReadView[object], start, end int64, acc byte) byte {
		// Same batch length shifted to a contiguous region.
		seg1, _ := rv.GetSegments(0, end-start)
		for i := range seg1 {
			acc ^= seg1[i].x[0]
		}
		return acc
	})
}

func BenchmarkReadView_Get(b *testing.B) {
	benchReadPath(b, func(rv ReadView[object], start, end int64, acc byte) byte {
		for seq := start; seq <= end; seq++ {
			acc ^= rv.Get(seq).x[0]
		}
		return acc
	})
}

func BenchmarkReadView_Iterate(b *testing.B) {
	benchReadPath(b, func(rv ReadView[object], start, end int64, acc byte) byte {
		for p := range rv.Iterate(start, end) {
			acc ^= p.x[0]
		}
		return acc
	})
}

// LockOSThread prevents the Go scheduler from moving the goroutine to a different
// M (OS thread). It does not set CPU affinity, but in practice the OS scheduler
// tends to keep a locked thread on the same core, reducing cross-core cache
// traffic and preemption overhead.
//
// Run this alongside BenchmarkRingBuffer_Publish_MultiReader to compare:
//
//	go test -run=^$ -bench='BenchmarkRingBuffer_Publish_MultiReader' -count=10 . | tee out.txt
//	go test -run=^$ -bench='BenchmarkRingBuffer_Publish_MultiReader_Locked' -count=10 . | tee -a out.txt
//	benchstat out.txt
func BenchmarkRingBuffer_Publish_MultiReader_Locked(b *testing.B) {
	readerCounts := []int{1, 2, 4, 8, 16, 32, 64, 128}

	for _, readers := range readerCounts {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			// Lock the writer (benchmark) goroutine to its OS thread.
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			const capacity = 1 << 22
			rb, err := NewRingBuffer[object](ctx, capacity)
			if err != nil {
				b.Fatal(err)
			}

			lockedReader := func(ctx context.Context, readView ReadView[object], readerCursor *atomic.Int64) {
				// Lock each reader goroutine to its own OS thread.
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()

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

			for range readers {
				if _, err := rb.barrier.AddReader(lockedReader); err != nil {
					b.Fatal(err)
				}
			}

			for b.Loop() {
				rb.PublishFunc(produce)
			}
		})
	}
}
