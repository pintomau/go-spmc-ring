package ringring

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	readerStarted := make(chan struct{})
	rb.barrier.AddReader(func(ctx context.Context, readView ReadView[T], readerCursor *atomic.Int64) {
		defer close(readerDone)
		close(readerStarted)
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

	// Wait until the pool has invoked the ReaderFunc: a cancel that lands
	// before then makes runSlot exit without calling it, so readerDone
	// would never close.
	<-readerStarted

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

	var started sync.WaitGroup
	started.Add(len(lags))

	for i, lag := range lags {
		done[i] = make(chan struct{})
		readerLag := lag
		readerDone := done[i]

		rb.barrier.AddReader(func(ctx context.Context, readView ReadView[SmallEvent], readerCursor *atomic.Int64) {
			defer close(readerDone)
			started.Done()
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

	// Wait until the pool has invoked every ReaderFunc: a cancel that lands
	// before then makes runSlot exit without calling it, so the done
	// channels would never close.
	started.Wait()

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

// Direct buffer access benchmarks (bypassing RingBuffer API)
// to isolate false sharing effects

// Benchmark: Single writer, single reader with direct buffer access
func benchmarkDirectBufferAccess[T any](b *testing.B, readerLag int64) {
	const capacity = 1 << 16
	buffer := make([]T, capacity)
	mask := int64(capacity - 1)

	var writerPos atomic.Int64
	var readerPos atomic.Int64
	writerPos.Store(0)
	readerPos.Store(-readerLag)

	var wg sync.WaitGroup
	done := make(chan struct{})
	readerDone := make(chan time.Duration)

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		readerStart := time.Now()
		r := readerPos.Load()

		for {
			select {
			case <-done:
				// Writer finished, read remaining elements
				finalWriterPos := writerPos.Load()
				for r < finalWriterPos {
					_ = buffer[r&mask]
					r++
				}
				readerPos.Store(r)
				readerDone <- time.Since(readerStart)
				return
			default:
				w := writerPos.Load()

				// Stay readerLag behind
				target := w - readerLag
				if target > r {
					// Actually read from buffer (forces cache load)
					for r < target {
						_ = buffer[r&mask]
						r++
					}
					readerPos.Store(r)
				}
			}
		}
	}()

	// Give reader time to start
	runtime.Gosched()

	// Writer: write to buffer
	for b.Loop() {
		seq := writerPos.Load()
		buffer[seq&mask] = *new(T) // Write to buffer
		writerPos.Store(seq + 1)
	}

	// Signal writer is done
	close(done)

	// Wait for reader to finish and get its duration
	readerDuration := <-readerDone
	wg.Wait()

	// Report reader metrics
	readerOpsPerSec := float64(b.N) / readerDuration.Seconds()
	readerNsPerOp := float64(readerDuration.Nanoseconds()) / float64(b.N)

	b.ReportMetric(readerNsPerOp, "reader-ns/op")
	b.ReportMetric(readerOpsPerSec, "reader-ops/s")
}

func BenchmarkDirectBuffer_Small_CloseLag(b *testing.B) {
	benchmarkDirectBufferAccess[SmallEvent](b, 10)
}

func BenchmarkDirectBuffer_Small_FarLag(b *testing.B) {
	benchmarkDirectBufferAccess[SmallEvent](b, 1000)
}

func BenchmarkDirectBuffer_Large_CloseLag(b *testing.B) {
	benchmarkDirectBufferAccess[LargeEvent](b, 10)
}

func BenchmarkDirectBuffer_Padded_CloseLag(b *testing.B) {
	benchmarkDirectBufferAccess[VeryLargeEvent](b, 10)
}

func BenchmarkDirectBuffer_Padded_FarLag(b *testing.B) {
	benchmarkDirectBufferAccess[VeryLargeEvent](b, 1000)
}
