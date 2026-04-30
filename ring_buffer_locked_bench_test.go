package ringring

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

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

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
