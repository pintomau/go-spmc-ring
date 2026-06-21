package spmc

import (
	"context"
	"sync/atomic"
	"testing"
)

// parkedReader returns a ReaderFunc that never reads. It hands its cursor to
// the test through curCh and parks until the context is canceled. The test
// advances the cursor directly, which keeps gating state fully deterministic
// (no reader-side polling or sleeps).
func parkedReader(curCh chan *atomic.Int64) ReaderFunc[int] {
	return func(ctx context.Context, _ ReadView[int], cur *atomic.Int64) {
		curCh <- cur
		<-ctx.Done()
	}
}

func TestRingBuffer_TryPublish_Basic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	curCh := make(chan *atomic.Int64, 1)
	if _, err := rb.barrier.AddReader(parkedReader(curCh)); err != nil {
		t.Fatal(err)
	}
	cur := <-curCh

	// Reader parked at 0: sequences 1..7 fit (seq < 0+8).
	for i := 1; i <= 7; i++ {
		if !rb.TryPublish(i) {
			t.Fatalf("TryPublish(%d) = false, want true", i)
		}
	}
	if w := rb.writeCursor.Load(); w != 7 {
		t.Fatalf("writeCursor = %d, want 7", w)
	}

	// Ring full: sequence 8 >= 0+8.
	if rb.TryPublish(8) {
		t.Fatal("TryPublish on full ring = true, want false")
	}
	if w := rb.writeCursor.Load(); w != 7 {
		t.Fatalf("writeCursor = %d after failed TryPublish, want 7", w)
	}
	if rb.nextSequence != 7 {
		t.Fatalf("nextSequence = %d after failed TryPublish, want 7", rb.nextSequence)
	}

	// Advance the reader; the next TryPublish must observe it via the
	// barrier refresh and succeed.
	cur.Store(1)
	if !rb.TryPublish(8) {
		t.Fatal("TryPublish after reader advanced = false, want true")
	}
	if w := rb.writeCursor.Load(); w != 8 {
		t.Fatalf("writeCursor = %d, want 8", w)
	}
	if got := rb.buffer[8&rb.mask]; got != 8 {
		t.Fatalf("buffer slot for seq 8 = %d, want 8", got)
	}
}

func TestRingBuffer_TryPublishFunc_FullRingDoesNotInvoke(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	curCh := make(chan *atomic.Int64, 1)
	if _, err := rb.barrier.AddReader(parkedReader(curCh)); err != nil {
		t.Fatal(err)
	}
	cur := <-curCh

	for i := 1; i <= 7; i++ {
		v := i
		if !rb.TryPublishFunc(func(slot *int) { *slot = v }) {
			t.Fatalf("TryPublishFunc #%d = false, want true", i)
		}
	}

	// Full ring: f must not be invoked.
	called := false
	if rb.TryPublishFunc(func(*int) { called = true }) {
		t.Fatal("TryPublishFunc on full ring = true, want false")
	}
	if called {
		t.Fatal("f was invoked on a full ring")
	}

	cur.Store(1)
	if !rb.TryPublishFunc(func(slot *int) { *slot = 42 }) {
		t.Fatal("TryPublishFunc after reader advanced = false, want true")
	}
	if got := rb.buffer[8&rb.mask]; got != 42 {
		t.Fatalf("buffer slot for seq 8 = %d, want 42", got)
	}
}

func TestRingBuffer_TryReserve_SuccessAndWrap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No readers: the barrier follows the writer cursor, so the ring is
	// never full and every TryReserve succeeds.
	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	// Non-wrapping reservation: seqs 1..5 map to buffer[1:6].
	seg1, seg2, claim, ok := rb.TryReserve(5)
	if !ok {
		t.Fatal("TryReserve(5) ok = false, want true")
	}
	if seg2 != nil {
		t.Fatalf("seg2 len = %d for non-wrapping reservation, want nil", len(seg2))
	}
	if len(seg1) != 5 || claim != 5 {
		t.Fatalf("seg1 len = %d, claim = %d; want 5, 5", len(seg1), claim)
	}
	for i := range seg1 {
		seg1[i] = 100 + i
	}
	if w := rb.writeCursor.Load(); w != 0 {
		t.Fatalf("writeCursor before Commit = %d, want 0", w)
	}
	rb.Commit(claim)
	if w := rb.writeCursor.Load(); w != 5 {
		t.Fatalf("writeCursor after Commit = %d, want 5", w)
	}

	// Wrapping reservation: seqs 6..9. start = 6&7 = 6, so seg1 = buffer[6:8]
	// (2 slots) and seg2 = buffer[0:2] (2 slots).
	seg1, seg2, claim, ok = rb.TryReserve(4)
	if !ok {
		t.Fatal("TryReserve(4) ok = false, want true")
	}
	if len(seg1) != 2 || len(seg2) != 2 || claim != 9 {
		t.Fatalf("seg1 len = %d, seg2 len = %d, claim = %d; want 2, 2, 9",
			len(seg1), len(seg2), claim)
	}
	rb.Commit(claim)
}

func TestRingBuffer_TryReserve_Full(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	curCh := make(chan *atomic.Int64, 1)
	if _, err := rb.barrier.AddReader(parkedReader(curCh)); err != nil {
		t.Fatal(err)
	}
	cur := <-curCh

	rb.PublishBatch([]int{1, 2, 3, 4, 5})

	// Reader parked at 0, writer at 5. A batch of 4 needs lastSeq 9 >= 0+8: full.
	seg1, seg2, claim, ok := rb.TryReserve(4)
	if ok {
		t.Fatal("TryReserve(4) ok = true on full ring, want false")
	}
	if seg1 != nil || seg2 != nil || claim != 0 {
		t.Fatalf("failed TryReserve leaked state: seg1=%v seg2=%v claim=%d", seg1, seg2, claim)
	}
	if rb.nextSequence != 5 {
		t.Fatalf("nextSequence = %d after failed TryReserve, want 5", rb.nextSequence)
	}

	// A batch of 3 needs lastSeq 8 >= 0+8: still full.
	if _, _, _, ok := rb.TryReserve(3); ok {
		t.Fatal("TryReserve(3) ok = true, want false")
	}
	// A batch of 2 needs lastSeq 7 < 0+8: fits.
	_, _, claim, ok = rb.TryReserve(2)
	if !ok {
		t.Fatal("TryReserve(2) ok = false, want true")
	}
	rb.Commit(claim)

	// Advancing the reader frees room for the batch of 4 (lastSeq 11 < 4+8).
	cur.Store(4)
	if _, _, _, ok := rb.TryReserve(4); !ok {
		t.Fatal("TryReserve(4) after reader advanced = false, want true")
	}
}

func TestRingBuffer_TryReserve_PanicsOnBadSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	cases := []int64{0, -1, 8, 9, 100}
	for _, n := range cases {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("TryReserve(%d) did not panic", n)
				}
			}()
			rb.TryReserve(n)
		}()
	}
}

func TestRingBuffer_Remaining(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("no readers", func(t *testing.T) {
		rb, err := NewRingBuffer[int](ctx, 8)
		if err != nil {
			t.Fatal(err)
		}
		// Usable capacity is bufferSize-1 under the strict < gating.
		if got := rb.Remaining(); got != 7 {
			t.Fatalf("Remaining = %d on empty ring, want 7", got)
		}
		// With no readers the barrier follows the writer: always 7.
		rb.PublishBatch([]int{1, 2, 3})
		if got := rb.Remaining(); got != 7 {
			t.Fatalf("Remaining = %d with no readers after publish, want 7", got)
		}
	})

	t.Run("stalled reader", func(t *testing.T) {
		rb, err := NewRingBuffer[int](ctx, 8)
		if err != nil {
			t.Fatal(err)
		}
		curCh := make(chan *atomic.Int64, 1)
		if _, err := rb.barrier.AddReader(parkedReader(curCh)); err != nil {
			t.Fatal(err)
		}
		cur := <-curCh

		if got := rb.Remaining(); got != 7 {
			t.Fatalf("Remaining = %d with caught-up reader, want 7", got)
		}
		rb.PublishBatch([]int{1, 2, 3})
		if got := rb.Remaining(); got != 4 {
			t.Fatalf("Remaining = %d after 3 published, want 4", got)
		}
		rb.PublishBatch([]int{4, 5, 6, 7})
		if got := rb.Remaining(); got != 0 {
			t.Fatalf("Remaining = %d on full ring, want 0", got)
		}
		// Remaining == 0 must coincide exactly with TryPublish failing.
		if rb.TryPublish(8) {
			t.Fatal("TryPublish = true while Remaining = 0")
		}
		cur.Store(2)
		if got := rb.Remaining(); got != 2 {
			t.Fatalf("Remaining = %d after reader advanced to 2, want 2", got)
		}
		if !rb.TryPublish(8) {
			t.Fatal("TryPublish = false while Remaining > 0")
		}
	})
}

func TestRingBuffer_TryPublish_PipelineGating(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	s1 := rb.NewStage(nil)          // gated by the writer cursor
	s2 := rb.NewStage(s1.Barrier()) // gated by s1's min cursor
	rb.SetGatingStage(s2)           // writer waits for the leaf stage

	// Both stages get parked readers whose cursors the test advances by hand,
	// so every gating observation below is deterministic. Note s2's barrier is
	// min(s1's min, s2's readers): the leaf stage can never run ahead of s1.
	s1CurCh := make(chan *atomic.Int64, 1)
	if _, err := s1.AddReader(parkedReader(s1CurCh)); err != nil {
		t.Fatal(err)
	}
	s1Cur := <-s1CurCh

	s2CurCh := make(chan *atomic.Int64, 1)
	if _, err := s2.AddReader(parkedReader(s2CurCh)); err != nil {
		t.Fatal(err)
	}
	s2Cur := <-s2CurCh

	for i := 1; i <= 7; i++ {
		if !rb.TryPublish(i) {
			t.Fatalf("TryPublish(%d) = false, want true", i)
		}
	}

	// Stage 1 is fully caught up, but the parked leaf reader holds the ring full.
	s1Cur.Store(7)
	if rb.TryPublish(8) {
		t.Fatal("TryPublish = true while leaf-stage reader is parked at 0")
	}
	if got := rb.Remaining(); got != 0 {
		t.Fatalf("Remaining = %d while gated by leaf stage, want 0", got)
	}

	// Advancing the leaf reader ungates the writer.
	s2Cur.Store(2)
	if !rb.TryPublish(8) {
		t.Fatal("TryPublish = false after leaf reader advanced")
	}

	// The converse also holds: if s1 stalls behind s2, s1 gates the writer.
	// s1 at 7 caps the barrier even though s2 jumps to 8.
	s2Cur.Store(8)
	for i := 9; i <= 14; i++ {
		if !rb.TryPublish(i) {
			t.Fatalf("TryPublish(%d) = false, want true", i)
		}
	}
	if rb.TryPublish(15) {
		t.Fatal("TryPublish = true while stage-1 reader is parked at 7")
	}

	s1.Shutdown()
	s2.Shutdown()
	rb.barrier.Shutdown()
}
