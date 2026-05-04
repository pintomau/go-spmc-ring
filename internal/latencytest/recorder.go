package latencytest

import (
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// LatencyRecorder holds three HDR histograms covering 1ns–10s at 3 significant
// figures. One instance per scenario.
type LatencyRecorder struct {
	WriterStall *hdrhistogram.Histogram // PublishedAt − EnqueuedAt
	ReaderLag   *hdrhistogram.Histogram // ConsumedAt  − PublishedAt
	EndToEnd    *hdrhistogram.Histogram // ConsumedAt  − EnqueuedAt
}

func NewLatencyRecorder() *LatencyRecorder {
	return &LatencyRecorder{
		WriterStall: hdrhistogram.New(1, int64(10*time.Second), 3),
		ReaderLag:   hdrhistogram.New(1, int64(10*time.Second), 3),
		EndToEnd:    hdrhistogram.New(1, int64(10*time.Second), 3),
	}
}

// RecordConsume records the three latency measurements for a single consumed
// payload. The caller provides consumedAt (time.Now().UnixNano()) so this
// function avoids an extra clock call.
func (r *LatencyRecorder) RecordConsume(p Payload, consumedAt int64) {
	writerStall := p.PublishedAt - p.EnqueuedAt
	readerLag := consumedAt - p.PublishedAt
	e2e := consumedAt - p.EnqueuedAt
	if writerStall > 0 {
		_ = r.WriterStall.RecordValue(writerStall)
	}
	if readerLag > 0 {
		_ = r.ReaderLag.RecordValue(readerLag)
	}
	if e2e > 0 {
		_ = r.EndToEnd.RecordValue(e2e)
	}
}
