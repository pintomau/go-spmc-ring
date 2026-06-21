package ringring

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// spinReader advances the cursor to the writer barrier on every iteration without
// reading individual events. Used in pipeline tests where readers must keep up
// without sleeping. Do not use as a template for readers that call rv.Get; the
// bulk-advance skips per-sequence processing.
func spinReader(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if w := rv.LoadWriterBarrier(); cur.Load() < w {
				cur.Store(w)
			}
		}
	}
}

// TestPipeline_2Stage_Stage2BlockedByStage1 verifies that stage 2 readers
// cannot advance past what stage 1 has committed.
func TestPipeline_2Stage_Stage2BlockedByStage1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 64)
	if err != nil {
		t.Fatal(err)
	}

	s1 := rb.NewStage(nil)
	s2 := rb.NewStage(s1.Barrier())
	rb.SetGatingStage(s2)

	const N = 10

	// Stage 1 reader blocks until s1Unblock is closed, then drains all N events.
	s1Unblock := make(chan struct{})
	s1Done := make(chan struct{})
	if _, err := s1.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		select {
		case <-ctx.Done():
			return
		case <-s1Unblock:
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if w := rv.LoadWriterBarrier(); w >= N {
					cur.Store(w)
					close(s1Done)
					return
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Stage 2 reader spins and records the highest sequence it has seen.
	var s2MaxSeen atomic.Int64
	if _, err := s2.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if w := rv.LoadWriterBarrier(); cur.Load() < w {
					s2MaxSeen.Store(w)
					cur.Store(w)
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Publish N events. Buffer is 64, so this never stalls even with stage 1 blocked.
	for i := range N {
		rb.Publish(i)
	}

	// Give stage 2 time to spin; it must see nothing while stage 1 is at 0.
	time.Sleep(20 * time.Millisecond)
	if got := s2MaxSeen.Load(); got != 0 {
		t.Fatalf("stage 2 advanced to %d while stage 1 was blocked at 0", got)
	}

	// Unblock stage 1 and wait for it to drain.
	close(s1Unblock)
	select {
	case <-s1Done:
	case <-time.After(2 * time.Second):
		t.Fatal("stage 1 timed out draining events")
	}

	// Stage 2 should now advance.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s2MaxSeen.Load() >= N {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := s2MaxSeen.Load(); got < N {
		t.Fatalf("stage 2 only advanced to %d after stage 1 completed, want %d", got, N)
	}
}

// TestPipeline_WriterBackpressure verifies that the writer stalls when the leaf
// stage's cursor falls too far behind (buffer full), and unblocks when stage 1 advances.
func TestPipeline_WriterBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// bufferSize=8: writer can hold sequences 1..7 while stage 1 is at 0.
	// Sequence 8 stalls (8 < 0+8 is false).
	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	s1 := rb.NewStage(nil)
	s2 := rb.NewStage(s1.Barrier())
	rb.SetGatingStage(s2)

	// Stage 2 spins, kept up with whatever stage 1 allows.
	if _, err := s2.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		spinReader(ctx, rv, cur)
	}); err != nil {
		t.Fatal(err)
	}

	// Stage 1 blocks until s1Unblock is closed.
	s1Unblock := make(chan struct{})
	var s1UnblockOnce sync.Once
	unblockS1 := func() { s1UnblockOnce.Do(func() { close(s1Unblock) }) }
	t.Cleanup(unblockS1)
	if _, err := s1.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		select {
		case <-ctx.Done():
			return
		case <-s1Unblock:
		}
		// drain after unblock
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if w := rv.LoadWriterBarrier(); cur.Load() < w {
					cur.Store(w)
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Publish 7 events synchronously; these fit in the buffer while stage 1 is at 0.
	for i := range 7 {
		rb.Publish(i)
	}

	// The 8th event must stall. Publish it in a goroutine and verify it blocks.
	publishDone := make(chan struct{})
	go func() {
		rb.Publish(99)
		close(publishDone)
	}()

	select {
	case <-publishDone:
		t.Fatal("writer published 8th event immediately; should have stalled")
	case <-time.After(20 * time.Millisecond):
		// Good: writer is stalled.
	}

	// Unblock stage 1. Stage 2 (spinning) will advance as soon as stage 1 does.
	// Writer can then complete the 8th event.
	unblockS1()
	select {
	case <-publishDone:
		// Good.
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not unblock after stage 1 advanced")
	}
}

// TestPipeline_EmptyStagePassthrough verifies that a stage with no readers does not
// deadlock; its barrier propagates the upstream position unchanged.
func TestPipeline_EmptyStagePassthrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 64)
	if err != nil {
		t.Fatal(err)
	}

	s1 := rb.NewStage(nil)
	s2 := rb.NewStage(s1.Barrier()) // no readers added to s2
	s3 := rb.NewStage(s2.Barrier())
	rb.SetGatingStage(s3)

	// s1 and s3 have readers; s2 has none.
	if _, err := s1.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		spinReader(ctx, rv, cur)
	}); err != nil {
		t.Fatal(err)
	}

	s3Done := make(chan struct{})
	const N = 20
	if _, err := s3.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if w := rv.LoadWriterBarrier(); cur.Load() < w {
					cur.Store(w)
					if w >= N {
						close(s3Done) // single-goroutine reader; cannot be called twice
					}
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	for i := range N {
		rb.Publish(i)
	}

	select {
	case <-s3Done:
		// Good: s3 advanced through the empty s2 without deadlocking.
	case <-time.After(2 * time.Second):
		t.Fatal("stage 3 timed out; empty stage 2 may have caused a deadlock")
	}
}

// TestPipeline_3Stage_Ordering verifies that blocking stage 1 freezes stages 2 and 3.
func TestPipeline_3Stage_Ordering(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 64)
	if err != nil {
		t.Fatal(err)
	}

	s1 := rb.NewStage(nil)
	s2 := rb.NewStage(s1.Barrier())
	s3 := rb.NewStage(s2.Barrier())
	rb.SetGatingStage(s3)

	const N = 10

	s1Unblock := make(chan struct{})
	s1Done := make(chan struct{})
	if _, err := s1.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		select {
		case <-ctx.Done():
			return
		case <-s1Unblock:
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if w := rv.LoadWriterBarrier(); w >= N {
					cur.Store(w)
					close(s1Done)
					return
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	// s2 and s3 spin.
	var s2MaxSeen, s3MaxSeen atomic.Int64
	if _, err := s2.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if w := rv.LoadWriterBarrier(); cur.Load() < w {
					s2MaxSeen.Store(w)
					cur.Store(w)
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := s3.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if w := rv.LoadWriterBarrier(); cur.Load() < w {
					s3MaxSeen.Store(w)
					cur.Store(w)
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	for i := range N {
		rb.Publish(i)
	}

	// Neither s2 nor s3 should advance while s1 is blocked.
	time.Sleep(20 * time.Millisecond)
	if got := s2MaxSeen.Load(); got != 0 {
		t.Errorf("stage 2 advanced to %d while stage 1 blocked", got)
	}
	if got := s3MaxSeen.Load(); got != 0 {
		t.Errorf("stage 3 advanced to %d while stage 1 blocked", got)
	}

	// Unblock s1; s2 and s3 should both catch up.
	close(s1Unblock)
	select {
	case <-s1Done:
	case <-time.After(2 * time.Second):
		t.Fatal("stage 1 timed out")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s2MaxSeen.Load() >= N && s3MaxSeen.Load() >= N {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := s2MaxSeen.Load(); got < N {
		t.Errorf("stage 2 only reached %d, want %d", got, N)
	}
	if got := s3MaxSeen.Load(); got < N {
		t.Errorf("stage 3 only reached %d, want %d", got, N)
	}
}
