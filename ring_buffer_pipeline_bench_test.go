package ringring

// Run pipeline benchmarks without the simulation test suite:
//
//	go test -bench=BenchmarkPipeline -run=^$ -benchtime=3s ./...
//
// For statistical comparison with benchstat:
//
//	go test -bench=BenchmarkPipeline -run=^$ -benchtime=3s -count=10 ./... | tee out.txt
//	benchstat out.txt

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// pipelineReader is a fast drain reader for pipeline benchmarks. It uses a short
// sleep only when truly idle so it doesn't dominate CPU and stall downstream stages.
func pipelineReader(ctx context.Context, rv ReadView[object], cur *atomic.Int64) {
	current := cur.Load()
	var spins int
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if w := rv.LoadWriterBarrier(); current < w {
			for seq := current + 1; seq <= w; seq++ {
				obj = rv.Get(seq)
			}
			cur.Store(w)
			current = w
			spins = 0
		} else {
			spins++
			if spins > 200 {
				time.Sleep(time.Microsecond)
				spins = 0
			} else {
				runtime.Gosched()
			}
		}
	}
}

// BenchmarkPipeline_1Stage is the pre-pipeline baseline. Readers use pipelineReader
// (same as pipeline stages) so the comparison is apples-to-apples.
func BenchmarkPipeline_1Stage(b *testing.B) {
	for _, readers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			rb, err := NewRingBuffer[object](ctx, 1<<22)
			if err != nil {
				b.Fatal(err)
			}

			for range readers {
				if _, err := rb.barrier.AddReader(pipelineReader); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for b.Loop() {
				rb.PublishFunc(produce)
			}
		})
	}
}

// BenchmarkPipeline_1Stage_Approach2 uses the Approach 2 API with a single stage.
func BenchmarkPipeline_1Stage_Approach2(b *testing.B) {
	for _, readers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			rb, err := NewRingBuffer[object](ctx, 1<<22)
			if err != nil {
				b.Fatal(err)
			}

			s1 := rb.NewStage(nil)
			rb.SetGatingStage(s1)

			for range readers {
				if _, err := s1.AddReader(pipelineReader); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for b.Loop() {
				rb.PublishFunc(produce)
			}
		})
	}
}

// BenchmarkPipeline_2Stage_Approach2 exercises the explicit Stage[T] path:
// NewStage + Barrier() + SetGatingStage.
func BenchmarkPipeline_2Stage_Approach2(b *testing.B) {
	for _, readers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			rb, err := NewRingBuffer[object](ctx, 1<<22)
			if err != nil {
				b.Fatal(err)
			}

			s1 := rb.NewStage(nil)
			s2 := rb.NewStage(s1.Barrier())
			rb.SetGatingStage(s2)

			for range readers {
				if _, err := s1.AddReader(pipelineReader); err != nil {
					b.Fatal(err)
				}
				if _, err := s2.AddReader(pipelineReader); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for b.Loop() {
				rb.PublishFunc(produce)
			}
		})
	}
}

// BenchmarkPipeline_3Stage_Approach2 shows how overhead scales with pipeline depth.
func BenchmarkPipeline_3Stage_Approach2(b *testing.B) {
	for _, readers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			rb, err := NewRingBuffer[object](ctx, 1<<22)
			if err != nil {
				b.Fatal(err)
			}

			s1 := rb.NewStage(nil)
			s2 := rb.NewStage(s1.Barrier())
			s3 := rb.NewStage(s2.Barrier())
			rb.SetGatingStage(s3)

			for range readers {
				if _, err := s1.AddReader(pipelineReader); err != nil {
					b.Fatal(err)
				}
				if _, err := s2.AddReader(pipelineReader); err != nil {
					b.Fatal(err)
				}
				if _, err := s3.AddReader(pipelineReader); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for b.Loop() {
				rb.PublishFunc(produce)
			}
		})
	}
}
