package ringring

import (
	"context"
	"testing"
	"time"
)

func TestRingBuffer_WaitStrategy_Default(t *testing.T) {
	ctx := context.Background()
	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	if rb.writerWaitStrategy != WaitStrategyYield {
		t.Errorf("expected default strategy to be Yield, got %d", rb.writerWaitStrategy)
	}
}

func TestRingBuffer_WaitStrategy_Options(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		strategy WaitStrategy
	}{
		{"Spin", WaitStrategySpin},
		{"Yield", WaitStrategyYield},
		{"Sleep", WaitStrategySleep},
		{"Hybrid", WaitStrategyHybrid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb, err := NewRingBuffer[int](ctx, 8, WithWaitStrategy[int](tt.strategy))
			if err != nil {
				t.Fatal(err)
			}
			if rb.writerWaitStrategy != tt.strategy {
				t.Errorf("expected strategy %d, got %d", tt.strategy, rb.writerWaitStrategy)
			}
		})
	}
}

func TestRingBuffer_WaitStrategy_WithParams(t *testing.T) {
	ctx := context.Background()
	params := WaitStrategyParams{
		SleepDuration:       5 * time.Millisecond,
		HybridSpinCount:     50,
		HybridSleepDuration: 2 * time.Millisecond,
	}

	rb, err := NewRingBuffer[int](ctx, 8,
		WithWaitStrategy[int](WaitStrategySleep),
		WithWaitStrategyParams[int](params),
	)
	if err != nil {
		t.Fatal(err)
	}

	if rb.writerWaitStrategyParams.SleepDuration != 5*time.Millisecond {
		t.Errorf("expected SleepDuration 5ms, got %v", rb.writerWaitStrategyParams.SleepDuration)
	}
}

func TestRingBuffer_PublishWithStrategies(t *testing.T) {
	strategies := []WaitStrategy{
		WaitStrategySpin,
		WaitStrategyYield,
		WaitStrategySleep,
		WaitStrategyHybrid,
	}

	for _, strategy := range strategies {
		t.Run(strategy.String(), func(t *testing.T) {
			ctx := context.Background()
			rb, err := NewRingBuffer[int](ctx, 64, WithWaitStrategy[int](strategy))
			if err != nil {
				t.Fatal(err)
			}

			// Publish some values (no backpressure - happy path)
			for i := 0; i < 10; i++ {
				rb.Publish(i)
			}
		})
	}
}
