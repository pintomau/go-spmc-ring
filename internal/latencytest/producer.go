package latencytest

import (
	"context"
	"time"

	"github.com/pintomau/ringring"
)

// FixedRateProducer fires the producer on an independent time.Ticker so that
// consumer slowness does not mask tail latency (coordinated omission fix).
type FixedRateProducer struct {
	RateHz int
}

func (p FixedRateProducer) Run(ctx context.Context, rb *ringring.RingBuffer[Payload]) {
	interval := time.Duration(float64(time.Second) / float64(p.RateHz))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case firedAt := <-ticker.C:
			firedNano := firedAt.UnixNano()
			rb.PublishFunc(func(val *Payload) {
				val.EnqueuedAt = firedNano
				val.PublishedAt = firedNano + int64(time.Since(firedAt))
				val.Seq = seq
			})
			seq++
		}
	}
}

// BurstProducer fires burstSize events immediately, then idles for idleMs
// milliseconds, then repeats. Models bursty ingest patterns.
type BurstProducer struct {
	BurstSize int
	IdleMs    int
}

func (p BurstProducer) Run(ctx context.Context, rb *ringring.RingBuffer[Payload]) {
	idle := time.Duration(p.IdleMs) * time.Millisecond
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		now := time.Now()
		firedNano := now.UnixNano()
		for i := 0; i < p.BurstSize; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			s := seq
			rb.PublishFunc(func(val *Payload) {
				val.EnqueuedAt = firedNano
				val.PublishedAt = firedNano + int64(time.Since(now))
				val.Seq = s
			})
			seq++
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(idle):
		}
	}
}
