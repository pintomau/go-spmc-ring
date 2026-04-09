package ringring

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
)

// Test different element sizes to measure false sharing impact
//
// False sharing occurs when writer and reader access different memory locations
// that share the same CPU cache line, causing cache invalidation.
//
// On Apple Silicon (M-series): cache line = 128 bytes
// On most x86_64: cache line = 64 bytes

// SmallEvent: 8 bytes - HIGH false sharing risk
// If reader is 10 positions behind, likely sharing cache line with writer
type SmallEvent struct {
	Value [8]byte // 8 bytes of actual data
}

// MediumEvent: 32 bytes - MEDIUM false sharing risk
type MediumEvent struct {
	Value [32]byte
}

// LargeEvent: 64 bytes
type LargeEvent struct {
	Value [64]byte
}

// VeryLargeEvent: 128 bytes
type VeryLargeEvent struct {
	Value [128]byte
}

// Benchmark with single writer, single reader
func benchmarkBufferFalseSharing[T any](b *testing.B, newEvent func(*T), readerLag int64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 16 // 64K elements
	rb, err := NewRingBuffer[T](ctx, capacity)
	if err != nil {
		b.Fatal(err)
	}

	// Add a reader that lags behind writer by readerLag positions
	readerDone := make(chan struct{})
	rb.barrier.AddReader(func(ctx context.Context, readView ReadView[T], readerCursor *atomic.Int64) {
		defer close(readerDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				w := readView.LoadWriterBarrier()
				r := readerCursor.Load()

				// Stay readerLag positions behind writer
				target := w - readerLag
				if target > r {
					// Simulate reading/processing
					_ = readView.Get(target)
					readerCursor.Store(target)
					runtime.Gosched()
				}
			}
		}
	})

	// Give reader time to start
	runtime.Gosched()

	for b.Loop() {
		rb.PublishFunc(newEvent)
	}

	cancel()
	<-readerDone
}

// Benchmark: Small elements (8 bytes) - HIGH false sharing risk
func BenchmarkBuffer_SmallElement_CloseLag(b *testing.B) {
	benchmarkBufferFalseSharing(b, func(event *SmallEvent) {
		event.Value = [8]byte{}
	}, 10) // Reader 10 positions behind (likely same cache line)
}

func BenchmarkBuffer_SmallElement_MediumLag(b *testing.B) {
	benchmarkBufferFalseSharing(b, func(event *SmallEvent) {
		event.Value = [8]byte{}
	}, 100) // Reader 100 positions behind
}

func BenchmarkBuffer_SmallElement_FarLag(b *testing.B) {
	benchmarkBufferFalseSharing(b, func(event *SmallEvent) {
		event.Value = [8]byte{}
	}, 1000) // Reader far behind (different cache lines)
}

// Benchmark: Medium elements (32 bytes)
func BenchmarkBuffer_MediumElement_CloseLag(b *testing.B) {
	benchmarkBufferFalseSharing(b, func(event *MediumEvent) {
		event.Value = [32]byte{}
	}, 10)
}

func BenchmarkBuffer_MediumElement_MediumLag(b *testing.B) {
	benchmarkBufferFalseSharing(b, func(event *MediumEvent) {
		event.Value = [32]byte{}
	}, 100)
}

// Benchmark: Large elements (64 bytes)
func BenchmarkBuffer_LargeElement_CloseLag(b *testing.B) {
	benchmarkBufferFalseSharing(b, func(event *LargeEvent) {
		event.Value = [64]byte{}
	}, 10)
}

func BenchmarkBuffer_LargeElement_MediumLag(b *testing.B) {
	benchmarkBufferFalseSharing(b, func(event *LargeEvent) {
		event.Value = [64]byte{}
	}, 100)
}

// Benchmark: Very Large elements (128 bytes)
func BenchmarkBuffer_VeryLargeElement_CloseLag(b *testing.B) {
	benchmarkBufferFalseSharing(b, func(event *VeryLargeEvent) {
		event.Value = [128]byte{}
	}, 10)
}

func BenchmarkBuffer_VeryLargeElement_MediumLag(b *testing.B) {
	benchmarkBufferFalseSharing(b, func(event *VeryLargeEvent) {
		event.Value = [128]byte{}
	}, 100)
}

// Benchmark: Multiple readers at different positions
func BenchmarkBuffer_MultiReader_SmallElement(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const capacity = 1 << 16
	rb, _ := NewRingBuffer[SmallEvent](ctx, capacity)

	// Add 4 readers at different lag positions
	lags := []int64{10, 50, 200, 1000}
	done := make([]chan struct{}, len(lags))

	for i, lag := range lags {
		done[i] = make(chan struct{})
		readerLag := lag
		readerDone := done[i]

		rb.barrier.AddReader(func(ctx context.Context, readView ReadView[SmallEvent], readerCursor *atomic.Int64) {
			defer close(readerDone)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					w := readView.LoadWriterBarrier()
					r := readerCursor.Load()
					target := w - readerLag
					if target > r {
						_ = readView.Get(target)
						readerCursor.Store(target)
						runtime.Gosched()
					}
				}
			}
		})
	}

	// give time for readers to start
	runtime.Gosched()

	for b.Loop() {
		rb.PublishFunc(func(s *SmallEvent) {
			s.Value = [8]byte{}
		})
	}

	cancel()
	for _, ch := range done {
		<-ch
	}
}

// Pad64 is padded to 64 bytes.
// On machines with 128-byte cache lines (like Apple Silicon M-series),
// two adjacent Pad64 structs will share a single cache line.
type Pad64 struct {
	cursor atomic.Int64
	_      [56]byte
}

// Pad128 is padded to 128 bytes.
// This ensures that each struct occupies its own 128-byte cache line,
// preventing false sharing on Apple Silicon.
type Pad128 struct {
	cursor atomic.Int64
	_      [120]byte
}

var workerID atomic.Int64

func BenchmarkFalseSharing_Pad64(b *testing.B) {
	// Reset worker ID counter
	workerID.Store(-1)

	// Allocate enough slots for all potential workers
	slots := make([]Pad64, 1024)

	b.RunParallel(func(pb *testing.PB) {
		// Assign a unique slot to this worker
		id := workerID.Add(1)
		mySlot := &slots[id]

		for pb.Next() {
			mySlot.cursor.Add(1)
		}
	})
}

func BenchmarkFalseSharing_Pad128(b *testing.B) {
	// Reset worker ID counter
	workerID.Store(-1)

	// Allocate enough slots for all potential workers
	slots := make([]Pad128, 1024)

	b.RunParallel(func(pb *testing.PB) {
		// Assign a unique slot to this worker
		id := workerID.Add(1)
		mySlot := &slots[id]

		for pb.Next() {
			mySlot.cursor.Add(1)
		}
	})
}
