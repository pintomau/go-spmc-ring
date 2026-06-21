package spmc

import "testing"

// newTestReadView builds a ReadView over a buffer where value == index, so
// segment contents identify positions. writerCursor stays nil: these tests
// never call LoadWriterBarrier.
func newTestReadView(capacity int64) (ReadView[int64], []int64) {
	buf := make([]int64, capacity)
	for i := range buf {
		buf[i] = int64(i)
	}
	return ReadView[int64]{buffer: buf, mask: capacity - 1}, buf
}

func assertSeg(t *testing.T, name string, seg []int64, wantVals ...int64) {
	t.Helper()
	if int64(len(seg)) != int64(len(wantVals)) {
		t.Fatalf("%s: len = %d, want %d", name, len(seg), len(wantVals))
	}
	for i, w := range wantVals {
		if seg[i] != w {
			t.Fatalf("%s[%d] = %d, want %d", name, i, seg[i], w)
		}
	}
}

func TestReadView_GetSegments_NonWrapping(t *testing.T) {
	rv, buf := newTestReadView(8)
	seg1, seg2 := rv.GetSegments(2, 5)
	if seg2 != nil {
		t.Fatalf("seg2 = %v, want nil", seg2)
	}
	assertSeg(t, "seg1", seg1, 2, 3, 4, 5)
	if &seg1[0] != &buf[2] {
		t.Fatal("seg1 is not a zero-copy view into the buffer")
	}
}

func TestReadView_GetSegments_AbsoluteSequencesAreMasked(t *testing.T) {
	rv, buf := newTestReadView(8)
	// Sequences 10..13 mask to positions 2..5.
	seg1, seg2 := rv.GetSegments(10, 13)
	if seg2 != nil {
		t.Fatalf("seg2 = %v, want nil", seg2)
	}
	assertSeg(t, "seg1", seg1, 2, 3, 4, 5)
	if &seg1[0] != &buf[2] {
		t.Fatal("seg1 is not a zero-copy view into the buffer")
	}
}

func TestReadView_GetSegments_Wrapping(t *testing.T) {
	rv, buf := newTestReadView(8)
	// Sequences 6..9 mask to positions 6,7,0,1.
	seg1, seg2 := rv.GetSegments(6, 9)
	assertSeg(t, "seg1", seg1, 6, 7)
	assertSeg(t, "seg2", seg2, 0, 1)
	if &seg1[0] != &buf[6] || &seg2[0] != &buf[0] {
		t.Fatal("segments are not zero-copy views into the buffer")
	}
}

func TestReadView_GetSegments_SingleElement(t *testing.T) {
	rv, _ := newTestReadView(8)
	seg1, seg2 := rv.GetSegments(3, 3)
	if seg2 != nil {
		t.Fatalf("seg2 = %v, want nil", seg2)
	}
	assertSeg(t, "seg1", seg1, 3)
}

func TestReadView_GetSegments_MaxRange(t *testing.T) {
	rv, _ := newTestReadView(8)
	// bufferSize-1 = 7 events, the gating bound, wrapped: 5..11 masks to
	// 5,6,7 then 0,1,2,3.
	seg1, seg2 := rv.GetSegments(5, 11)
	assertSeg(t, "seg1", seg1, 5, 6, 7)
	assertSeg(t, "seg2", seg2, 0, 1, 2, 3)
}

func TestReadView_GetSegments_MaxRangeNonWrapping(t *testing.T) {
	rv, _ := newTestReadView(8)
	// bufferSize-1 = 7 events without wrapping: 0..6 stays in one segment.
	seg1, seg2 := rv.GetSegments(0, 6)
	if seg2 != nil {
		t.Fatalf("seg2 = %v, want nil", seg2)
	}
	assertSeg(t, "seg1", seg1, 0, 1, 2, 3, 4, 5, 6)
}

func collectIterate(rv ReadView[int64], start, end int64) []int64 {
	var got []int64
	for p := range rv.Iterate(start, end) {
		got = append(got, *p)
	}
	return got
}

func TestReadView_Iterate_InclusiveEnd(t *testing.T) {
	rv, _ := newTestReadView(8)
	assertSeg(t, "Iterate(2,5)", collectIterate(rv, 2, 5), 2, 3, 4, 5)
}

func TestReadView_Iterate_Wrapping(t *testing.T) {
	rv, _ := newTestReadView(8)
	assertSeg(t, "Iterate(6,9)", collectIterate(rv, 6, 9), 6, 7, 0, 1)
}

func TestReadView_Iterate_SingleElement(t *testing.T) {
	rv, _ := newTestReadView(8)
	assertSeg(t, "Iterate(3,3)", collectIterate(rv, 3, 3), 3)
}

func TestReadView_Iterate_EarlyStop(t *testing.T) {
	rv, _ := newTestReadView(8)
	// Range 6..9 yields seg1 (6,7) then seg2 (0,1). Stop inside each.
	for _, stopAfter := range []int{1, 3} {
		n := 0
		for range rv.Iterate(6, 9) {
			n++
			if n == stopAfter {
				break
			}
		}
		if n != stopAfter {
			t.Fatalf("stopAfter=%d: yielded %d times", stopAfter, n)
		}
	}
}
