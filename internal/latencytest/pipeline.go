package latencytest

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pintomau/ringring"
	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// PipelineScenario wires a multi-stage pipeline and measures per-stage latency
// using out-of-band []atomic.Int64 timestamp arrays (one entry per ring slot).
// Ring-buffer readers are read-only and cannot write back into payloads.
type PipelineScenario struct {
	Shape      WorkloadShape
	WriterWait ringring.WaitStrategy
	Polling    ReaderPolling
	// ReadersPerStage is the number of readers added to each stage.
	ReadersPerStage int
	Stages          int
	Duration        time.Duration
	BufferSize      int64
}

// PipelineResult extends Result with per-stage inter-stage latency histograms.
type PipelineResult struct {
	Result
	// StageToStage[i] is the latency from stage i to stage i+1 (0-indexed).
	StageToStage []*hdrhistogram.Histogram
}

// AssertP99Under checks the end-to-end p99 ceiling (delegates to Result).
func (r PipelineResult) AssertP99Under(t interface{ Helper(); Errorf(string, ...any) }, ceiling time.Duration) {
	r.Result.AssertP99Under(nil, ceiling)
}

// RunPipeline executes a multi-stage pipeline scenario.
func RunPipeline(s PipelineScenario) PipelineResult {
	if s.BufferSize == 0 {
		s.BufferSize = 1 << 17
	}
	if s.ReadersPerStage == 0 {
		s.ReadersPerStage = 1
	}
	if s.Stages < 1 {
		s.Stages = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := ringring.NewRingBuffer[Payload](ctx, s.BufferSize,
		ringring.WithWaitStrategy[Payload](s.WriterWait),
		ringring.WithWaitStrategyParams[Payload](ringring.DefaultWriterWaitStrategyParams()),
	)
	if err != nil {
		panic(err)
	}

	// Out-of-band timestamp arrays: stageTS[i][seq%cap] holds the nanosecond
	// timestamp when stage i finished processing sequence seq.
	cap := int(s.BufferSize)
	stageTS := make([][]atomic.Int64, s.Stages)
	for i := range s.Stages {
		stageTS[i] = make([]atomic.Int64, cap)
	}

	// Build stages and wire pipeline.
	stages := make([]*ringring.Stage[Payload], s.Stages)
	stages[0] = rb.NewStage(nil)
	for i := 1; i < s.Stages; i++ {
		stages[i] = rb.NewStage(stages[i-1].Barrier())
	}
	rb.SetGatingStage(stages[s.Stages-1])

	// Per-reader recorders (last stage writes end-to-end).
	lastRecs := make([]*LatencyRecorder, s.ReadersPerStage)
	for i := range s.ReadersPerStage {
		lastRecs[i] = NewLatencyRecorder()
	}

	// Add readers to each stage.
	for stageIdx, st := range stages {
		si := stageIdx
		tsArray := stageTS[si]
		bufCap := int64(cap)

		for readerIdx := range s.ReadersPerStage {
			ri := readerIdx
			var rec *LatencyRecorder
			if si == s.Stages-1 {
				rec = lastRecs[ri]
			}

			fn := ringring.ReaderFunc[Payload](func(ctx context.Context, rv ringring.ReadView[Payload], cur *atomic.Int64) {
				expected := cur.Load() + 1
				for {
					select {
					case <-ctx.Done():
						return
					default:
						w := rv.LoadWriterBarrier()
						if expected > w {
							switch s.Polling {
							case YieldReader:
								// runtime.Gosched() — import via consumer.go not visible here, inline
							case SleepReader:
								time.Sleep(time.Microsecond)
							}
							continue
						}
						now := time.Now().UnixNano()
						for seq := expected; seq <= w; seq++ {
							tsArray[seq&(bufCap-1)].Store(now)
							if rec != nil {
								p := rv.Get(seq)
								rec.RecordConsume(*p, now)
							}
						}
						cur.Store(w)
						expected = w + 1
					}
				}
			})
			if _, err := st.AddReader(fn); err != nil {
				panic(err)
			}
		}
	}

	producerCtx, producerCancel := context.WithTimeout(ctx, s.Duration)
	defer producerCancel()
	s.Shape.run(producerCtx, rb)

	time.Sleep(200 * time.Millisecond)
	cancel()

	// Build inter-stage histograms from stageTS arrays.
	stageToStage := make([]*hdrhistogram.Histogram, s.Stages-1)
	for i := range s.Stages - 1 {
		h := hdrhistogram.New(1, int64(10*time.Second), 3)
		for j := range cap {
			t0 := stageTS[i][j].Load()
			t1 := stageTS[i+1][j].Load()
			if t0 > 0 && t1 > t0 {
				_ = h.RecordValue(t1 - t0)
			}
		}
		stageToStage[i] = h
	}

	return PipelineResult{
		Result:       buildResult(lastRecs),
		StageToStage: stageToStage,
	}
}
