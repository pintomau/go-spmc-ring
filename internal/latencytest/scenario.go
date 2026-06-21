package latencytest

import (
	"context"
	"time"

	"github.com/pintomau/go-spmc-ring"
)

// WorkloadShape describes how the producer generates events.
type WorkloadShape interface {
	run(ctx context.Context, rb *spmc.RingBuffer[Payload])
}

// FixedRate fires the producer at a constant rate via time.Ticker.
type FixedRate struct{ RateHz int }

func (f FixedRate) run(ctx context.Context, rb *spmc.RingBuffer[Payload]) {
	FixedRateProducer(f).Run(ctx, rb)
}

// Burst fires BurstSize events immediately, idles IdleMs milliseconds, repeats.
type Burst struct {
	BurstSize int
	IdleMs    int
}

func (b Burst) run(ctx context.Context, rb *spmc.RingBuffer[Payload]) {
	BurstProducer(b).Run(ctx, rb)
}

// BurstReserve is like Burst but uses Reserve+Commit: the entire batch becomes
// visible atomically. BurstSize must be < BufferSize (default 131072).
type BurstReserve struct {
	BurstSize int
	IdleMs    int
}

func (b BurstReserve) run(ctx context.Context, rb *spmc.RingBuffer[Payload]) {
	BurstReserveProducer(b).Run(ctx, rb)
}

// Hetero scenario: one fast SpinReader + one slow reader with SleepPerEvent.
// The Readers field is always treated as 2.
type Hetero struct {
	RateHz        int
	SleepPerEvent time.Duration
}

func (h Hetero) run(ctx context.Context, rb *spmc.RingBuffer[Payload]) {
	FixedRateProducer{RateHz: h.RateHz}.Run(ctx, rb)
}

// Scenario is a single latency test run.
type Scenario struct {
	Shape      WorkloadShape
	WriterWait spmc.WaitStrategy
	Polling    ReaderPolling
	Readers    int
	Duration   time.Duration
	BufferSize int64
}

// Run executes the scenario and returns a Result.
func Run(s Scenario) Result {
	if s.BufferSize == 0 {
		s.BufferSize = 1 << 17 // 131072
	}
	if s.Readers == 0 {
		s.Readers = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := spmc.NewRingBuffer[Payload](ctx, s.BufferSize,
		spmc.WithWaitStrategy[Payload](s.WriterWait),
		spmc.WithDefaultWaitStrategyParams[Payload](),
	)
	if err != nil {
		panic(err)
	}

	stage := rb.NewStage(nil)
	rb.SetGatingStage(stage)

	readerCount := s.Readers
	if _, ok := s.Shape.(Hetero); ok {
		readerCount = 2
	}

	recs := make([]*LatencyRecorder, readerCount)
	for i := range readerCount {
		recs[i] = NewLatencyRecorder()
		var sleepPerEvent time.Duration
		if h, ok := s.Shape.(Hetero); ok && i > 0 {
			sleepPerEvent = h.SleepPerEvent
		}
		if _, err := stage.AddReader(makeConsumerFn(recs[i], s.Polling, sleepPerEvent)); err != nil {
			panic(err)
		}
	}

	producerCtx, producerCancel := context.WithTimeout(ctx, s.Duration)
	defer producerCancel()
	s.Shape.run(producerCtx, rb)

	// Give readers time to drain remaining ring-buffer slots before cancelling.
	time.Sleep(200 * time.Millisecond)
	cancel()

	return buildResult(recs)
}
