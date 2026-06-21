package spmc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// collectReader returns a ReaderFunc that records each item it reads into out
// (guarded by the atomic count). It advances its cursor to the writer barrier
// after each pass. done is closed once the reader has seen at least expected items.
func collectReader(t *testing.T, out *[]int, count *atomic.Int64, expected int64, done chan struct{}) ReaderFunc[int] {
	t.Helper()
	return func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		expectedSeq := cur.Load() + 1
		for {
			select {
			case <-ctx.Done():
				return
			default:
				w := rv.LoadWriterBarrier()
				if expectedSeq <= w {
					for seq := expectedSeq; seq <= w; seq++ {
						*out = append(*out, *rv.Get(seq))
					}
					cur.Store(w)
					n := count.Add(w - expectedSeq + 1)
					expectedSeq = w + 1
					if n >= expected {
						select {
						case <-done:
						default:
							close(done)
						}
					}
				} else {
					time.Sleep(10 * time.Microsecond)
				}
			}
		}
	}
}

func TestRingBuffer_PublishBatch_Basic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 64)
	if err != nil {
		t.Fatal(err)
	}

	var got []int
	var count atomic.Int64
	done := make(chan struct{})
	if _, err := rb.barrier.AddReader(collectReader(t, &got, &count, 10, done)); err != nil {
		t.Fatal(err)
	}

	rb.PublishBatch([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("reader did not see batch, count=%d", count.Load())
	}

	if len(got) != 10 {
		t.Fatalf("want 10 items, got %d", len(got))
	}
	for i, v := range got {
		if v != i+1 {
			t.Errorf("got[%d] = %d, want %d", i, v, i+1)
		}
	}
	if w := rb.writeCursor.Load(); w != 10 {
		t.Errorf("writeCursor = %d, want 10", w)
	}
}

func TestRingBuffer_PublishBatch_Wrap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// capacity = 8 so wraps happen quickly
	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	var got []int
	var count atomic.Int64
	done := make(chan struct{})
	if _, err := rb.barrier.AddReader(collectReader(t, &got, &count, 10, done)); err != nil {
		t.Fatal(err)
	}

	// First batch: seqs 1..6 (no wrap). Positions 1..6 in buffer.
	rb.PublishBatch([]int{1, 2, 3, 4, 5, 6})
	// Second batch: seqs 7..10. start = 7&7 = 7, length 4, so seg1=buffer[7:8] (1 slot)
	// and seg2=buffer[0:3] (3 slots). This exercises the wrap branch of Reserve.
	rb.PublishBatch([]int{7, 8, 9, 10})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("reader did not see wrap batch, count=%d", count.Load())
	}

	if len(got) != 10 {
		t.Fatalf("want 10 items, got %d", len(got))
	}
	for i, v := range got {
		if v != i+1 {
			t.Errorf("got[%d] = %d, want %d", i, v, i+1)
		}
	}
}

func TestRingBuffer_PublishBatchFunc_Indices(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 32)
	if err != nil {
		t.Fatal(err)
	}

	var got []int
	var count atomic.Int64
	done := make(chan struct{})
	if _, err := rb.barrier.AddReader(collectReader(t, &got, &count, 40, done)); err != nil {
		t.Fatal(err)
	}

	// Prime by publishing 28 items so the next batch wraps inside the callback.
	// capacity=32, so after writeCursor=28, a batch of 12 has start=29&31=29, end=29+12=41>32,
	// seg1=buffer[29:32] (3 slots), seg2=buffer[0:9] (9 slots). The callback must see i=0..11
	// in order regardless of the split.
	for k := 0; k < 28; k++ {
		rb.Publish(0)
	}
	// Wait for reader to catch up to writer so Reserve won't deadlock.
	// (Reader resets its position each pass; this works because capacity - 1 is the max headroom.)
	// Drain by waiting briefly.
	time.Sleep(10 * time.Millisecond)

	var indices []int64
	rb.PublishBatchFunc(12, func(i int64, slot *int) {
		indices = append(indices, i)
		*slot = int(100 + i)
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("reader did not see PublishBatchFunc output, count=%d", count.Load())
	}

	// Verify indices 0..11 in order.
	if len(indices) != 12 {
		t.Fatalf("callback fired %d times, want 12", len(indices))
	}
	for k, idx := range indices {
		if idx != int64(k) {
			t.Errorf("indices[%d] = %d, want %d", k, idx, k)
		}
	}

	// Last 12 items in got should be 100..111.
	tail := got[len(got)-12:]
	for k, v := range tail {
		if v != 100+k {
			t.Errorf("tail[%d] = %d, want %d", k, v, 100+k)
		}
	}
}

func TestRingBuffer_Reserve_Commit_Direct(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 16)
	if err != nil {
		t.Fatal(err)
	}

	seg1, seg2, claim := rb.Reserve(5)
	if seg2 != nil {
		t.Fatalf("seg2 should be nil for non-wrapping reservation, got len=%d", len(seg2))
	}
	if int64(len(seg1)) != 5 {
		t.Fatalf("seg1 len = %d, want 5", len(seg1))
	}
	if claim != 5 {
		t.Fatalf("claim = %d, want 5", claim)
	}
	for i := range seg1 {
		seg1[i] = 1000 + i
	}
	// Pre-commit, writeCursor must not advance.
	if w := rb.writeCursor.Load(); w != 0 {
		t.Fatalf("writeCursor before Commit = %d, want 0", w)
	}
	rb.Commit(claim)
	if w := rb.writeCursor.Load(); w != 5 {
		t.Fatalf("writeCursor after Commit = %d, want 5", w)
	}
	if rb.nextSequence != 5 {
		t.Errorf("nextSequence = %d, want 5", rb.nextSequence)
	}
}

func TestRingBuffer_Reserve_PanicsOnBadSize(t *testing.T) {
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
					t.Errorf("Reserve(%d) did not panic", n)
				}
			}()
			rb.Reserve(n)
		}()
	}
}

func TestRingBuffer_PublishBatch_Empty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	rb.PublishBatch(nil)
	rb.PublishBatch([]int{})
	if w := rb.writeCursor.Load(); w != 0 {
		t.Errorf("empty PublishBatch advanced writeCursor to %d", w)
	}
}

func TestRingBuffer_PublishBatch_Backpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := NewRingBuffer[int](ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	// Reader that holds at whatever position we force, controlled via advance chan.
	advance := make(chan int64, 8)
	readerCursor := make(chan *atomic.Int64, 1)
	stopped := make(chan struct{})
	_, err = rb.barrier.AddReader(func(ctx context.Context, rv ReadView[int], cur *atomic.Int64) {
		readerCursor <- cur
		defer close(stopped)
		for {
			select {
			case <-ctx.Done():
				return
			case target, ok := <-advance:
				if !ok {
					return
				}
				cur.Store(target)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	cur := <-readerCursor

	// Fill the ring: write 7 items (max that fits when reader is at 0: 7 < 0+8).
	rb.PublishBatch([]int{1, 2, 3, 4, 5, 6, 7})
	if cur.Load() != 0 {
		t.Fatalf("reader cursor = %d, want 0 (reader should not have advanced)", cur.Load())
	}

	// Next batch of 3 cannot fit: lastSeq=10, headroom = 0 + 8 = 8 → blocks.
	blocked := make(chan struct{})
	go func() {
		rb.PublishBatch([]int{8, 9, 10})
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("PublishBatch returned while buffer was full; should have blocked")
	case <-time.After(30 * time.Millisecond):
		// Expected: still blocked.
	}
	if w := rb.writeCursor.Load(); w != 7 {
		t.Fatalf("writeCursor advanced to %d while blocked; want 7", w)
	}

	// Drain: reader advances to 3. Now headroom = 3+8 = 11, batch (lastSeq=10) fits.
	advance <- 3

	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("PublishBatch did not unblock after reader advanced")
	}
	if w := rb.writeCursor.Load(); w != 10 {
		t.Errorf("writeCursor = %d after unblock, want 10", w)
	}

	close(advance)
	<-stopped
}
