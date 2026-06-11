package latencytest

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pintomau/ringring"
)

// ReaderPolling selects the polling strategy for consumer goroutines.
type ReaderPolling uint8

const (
	SpinReader  ReaderPolling = iota // pure busy-spin
	YieldReader                      // runtime.Gosched() each miss
	SleepReader                      // time.Sleep(1µs) each miss
	BatchReader                      // GetSegments per batch; single time.Now() per batch
)

func (r ReaderPolling) String() string {
	switch r {
	case SpinReader:
		return "Spin"
	case YieldReader:
		return "Yield"
	case SleepReader:
		return "Sleep"
	case BatchReader:
		return "Batch"
	default:
		return "Unknown"
	}
}

// makeConsumerFn returns a ReaderFunc that records all consumed events into rec.
// sleepPerEvent is added after each event for SleepReader / Hetero scenarios
// (pass 0 for normal consumers).
func makeConsumerFn(rec *LatencyRecorder, polling ReaderPolling, sleepPerEvent time.Duration) ringring.ReaderFunc[Payload] {
	if polling == BatchReader {
		return makeBatchConsumerFn(rec)
	}
	return func(ctx context.Context, rv ringring.ReadView[Payload], cur *atomic.Int64) {
		expected := cur.Load() + 1
		for {
			select {
			case <-ctx.Done():
				return
			default:
				w := rv.LoadWriterBarrier()
				if expected > w {
					switch polling {
					case YieldReader:
						runtime.Gosched()
					case SleepReader:
						time.Sleep(time.Microsecond)
					}
					continue
				}
				for seq := expected; seq <= w; seq++ {
					p := rv.Get(seq)
					consumedAt := time.Now().UnixNano()
					rec.RecordConsume(*p, consumedAt)
					if sleepPerEvent > 0 {
						cur.Store(seq) // advance per-event so the writer isn't gated for the full batch
						time.Sleep(sleepPerEvent)
					}
				}
				cur.Store(w)
				expected = w + 1
			}
		}
	}
}

// makeBatchConsumerFn returns a ReaderFunc that reads each available batch
// with rv.GetSegments and stamps all events in the batch with a single
// time.Now() call. Both segments are zero-copy views into the ring; seg2 is
// non-empty only when the batch wraps the ring end. All events in a batch
// share the same consumedAt, so per-event intra-batch latency is not
// visible.
func makeBatchConsumerFn(rec *LatencyRecorder) ringring.ReaderFunc[Payload] {
	return func(ctx context.Context, rv ringring.ReadView[Payload], cur *atomic.Int64) {
		expected := cur.Load() + 1
		for {
			select {
			case <-ctx.Done():
				return
			default:
				w := rv.LoadWriterBarrier()
				if expected > w {
					continue
				}
				seg1, seg2 := rv.GetSegments(expected, w)
				consumedAt := time.Now().UnixNano()
				for i := range seg1 {
					rec.RecordConsume(seg1[i], consumedAt)
				}
				for i := range seg2 {
					rec.RecordConsume(seg2[i], consumedAt)
				}
				cur.Store(w)
				expected = w + 1
			}
		}
	}
}
