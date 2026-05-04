package cdv_disruptor

import (
	"context"
	"sync/atomic"
	"testing"
)

// BenchmarkFalseSharing_Readers tests if adjacent readers cause cache line thrashing.
// If padding is correct, performance should be high (similar to _ControlNonSharing).
// If padding is incorrect (false sharing), performance will degrade (similar to _ControlSharing).
func BenchmarkFalseSharing_Readers(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cursor := &atomic.Int64{}
	pool := NewBitmapReaderPool[int](ctx, cursor)

	dummyFn := func(ctx context.Context, readView ReadView[int], readerCusor *atomic.Int64) {
		<-ctx.Done()
	}

	// Force slots 0 and 1 (adjacent in array)
	id1, err1 := pool.AddReader(dummyFn)
	id2, err2 := pool.AddReader(dummyFn)

	if err1 != nil || err2 != nil {
		b.Fatalf("Failed to add readers: %v, %v", err1, err2)
	}
	if id1 != 0 || id2 != 1 {
		b.Logf("Warning: Readers not adjacent (ids: %d, %d). False sharing test might be invalid.", id1, id2)
	}

	b.ResetTimer()
	b.SetParallelism(2)

	var toggle atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		which := toggle.Add(1)

		if which&1 == 1 {
			for pb.Next() {
				pool.slots[id1].cursor.Add(1) // Half goroutines hit slot 0
			}
		} else {
			for pb.Next() {
				pool.slots[id2].cursor.Add(1) // Half goroutines hit slot 1
			}
		}
	})

	// Cleanup
	pool.RemoveReader(id1)
	pool.RemoveReader(id2)
}

// paddedPair deliberately creates a non-false sharing for comparison
type paddedPair struct {
	_       [CacheLineSize]byte
	cursor1 atomic.Int64 // 8 bytes
	_       [CacheLineSize - 8]byte
	cursor2 atomic.Int64 // 8 bytes - wil NOT share cache line with cursor1
	_       [CacheLineSize - 8]byte
}

// BenchmarkFalseSharing_ControlNonSharing demonstrates a scenario where false sharing should be impossible.
// This should be FAST because cursor1 and cursor2 don't share a cache line.
func BenchmarkFalseSharing_ControlNonSharing(b *testing.B) {
	pair := &paddedPair{}

	b.ResetTimer()
	b.SetParallelism(2) // Force exactly 2 goroutines

	var toggle atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		// Split goroutines
		which := toggle.Add(1)

		if which&1 == 1 {
			for pb.Next() {
				pair.cursor1.Add(1) // Goroutine 1 hammers this
			}
		} else {
			for pb.Next() {
				pair.cursor2.Add(1) // Goroutine 2 hammers this
			}
		}
	})
}

// unpaddedPair deliberately creates false sharing for comparison
type unpaddedPair struct {
	cursor1 atomic.Int64 // 8 bytes
	cursor2 atomic.Int64 // 8 bytes - WILL share cache line with cursor1
}

// BenchmarkFalseSharing_ControlSharing demonstrates actual false sharing.
// This should be SLOW because cursor1 and cursor2 share a cache line.
func BenchmarkFalseSharing_ControlSharing(b *testing.B) {
	pair := &unpaddedPair{}

	b.ResetTimer()
	b.SetParallelism(2) // Force exactly 2 goroutines

	var toggle atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		// Split goroutines
		which := toggle.Add(1)

		if which&1 == 1 {
			for pb.Next() {
				pair.cursor1.Add(1) // Goroutine 1 hammers this
			}
		} else {
			for pb.Next() {
				pair.cursor2.Add(1) // Goroutine 2 hammers this
			}
		}
	})
}
