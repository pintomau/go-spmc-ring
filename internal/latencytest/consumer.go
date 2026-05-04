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
)

func (r ReaderPolling) String() string {
	switch r {
	case SpinReader:
		return "Spin"
	case YieldReader:
		return "Yield"
	case SleepReader:
		return "Sleep"
	default:
		return "Unknown"
	}
}

// makeConsumerFn returns a ReaderFunc that records all consumed events into rec.
// sleepPerEvent is added after each event for SleepReader / Hetero scenarios
// (pass 0 for normal consumers).
func makeConsumerFn(rec *LatencyRecorder, polling ReaderPolling, sleepPerEvent time.Duration) ringring.ReaderFunc[Payload] {
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
