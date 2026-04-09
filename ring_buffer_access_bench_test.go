package ringring

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
	wg.Go(func() {
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
	})

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
