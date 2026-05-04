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

// BurstProducer fires burstSize events immediately via individual PublishFunc
// calls, then idles for idleMs milliseconds, then repeats.
type BurstProducer struct {
	BurstSize int
	IdleMs    int
}

// BurstReserveProducer is like BurstProducer but uses Reserve+Commit so the
// entire burst becomes visible to readers in a single atomic writeCursor.Store.
// All slots in the batch share the same EnqueuedAt (burst start) and
// PublishedAt (just before Commit, after any stall in Reserve resolves).
// BurstSize must be < BufferSize.
type BurstReserveProducer struct {
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

func (p BurstReserveProducer) Run(ctx context.Context, rb *ringring.RingBuffer[Payload]) {
	idle := time.Duration(p.IdleMs) * time.Millisecond
	n := int64(p.BurstSize)
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		now := time.Now()
		enqueuedAt := now.UnixNano()

		// Reserve blocks until n contiguous slots are free; may wrap the ring.
		seg1, seg2, claim := rb.Reserve(n)

		// Set EnqueuedAt and Seq on every slot; PublishedAt is set after Reserve
		// returns so it captures the full stall time inside Reserve.
		publishedAt := enqueuedAt + int64(time.Since(now))
		for i := range seg1 {
			seg1[i].EnqueuedAt = enqueuedAt
			seg1[i].PublishedAt = publishedAt
			seg1[i].Seq = seq
			seq++
		}
		for i := range seg2 {
			seg2[i].EnqueuedAt = enqueuedAt
			seg2[i].PublishedAt = publishedAt
			seg2[i].Seq = seq
			seq++
		}
		rb.Commit(claim)

		select {
		case <-ctx.Done():
			return
		case <-time.After(idle):
		}
	}
}
