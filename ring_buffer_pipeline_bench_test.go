package ringring

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkPipeline_1Stage_NoPipeline is the direct-registration baseline. It
// uses keepUpReader rather than pipelineReader so it stays comparable to the
// core ring-buffer publish benchmarks in ring_buffer_bench_test.go.
func BenchmarkPipeline_1Stage_NoPipeline(b *testing.B) {
	for _, readers := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			rb, err := NewRingBuffer[object](ctx, 1<<22)
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

// BenchmarkPipeline_1Stage measures the single-stage Stage[T] API path.
func BenchmarkPipeline_1Stage(b *testing.B) {
	for _, readers := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
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
				if _, err := s1.AddReader(keepUpReader); err != nil {
					b.Fatal(err)
				}
			}

			for b.Loop() {
				rb.PublishFunc(produce)
			}
		})
	}
}

// BenchmarkPipeline_2Stage exercises the explicit Stage[T] path:
// NewStage + Barrier() + SetGatingStage.
func BenchmarkPipeline_2Stage(b *testing.B) {
	for _, readers := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
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
				if _, err := s1.AddReader(keepUpReader); err != nil {
					b.Fatal(err)
				}
				if _, err := s2.AddReader(keepUpReader); err != nil {
					b.Fatal(err)
				}
			}

			for b.Loop() {
				rb.PublishFunc(produce)
			}
		})
	}
}

// BenchmarkPipeline_3Stage shows how overhead scales with pipeline depth.
func BenchmarkPipeline_3Stage(b *testing.B) {
	for _, readers := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
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
				if _, err := s1.AddReader(keepUpReader); err != nil {
					b.Fatal(err)
				}
				if _, err := s2.AddReader(keepUpReader); err != nil {
					b.Fatal(err)
				}
				if _, err := s3.AddReader(keepUpReader); err != nil {
					b.Fatal(err)
				}
			}

			for b.Loop() {
				rb.PublishFunc(produce)
			}
		})
	}
}
