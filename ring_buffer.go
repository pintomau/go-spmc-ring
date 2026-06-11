package ringring

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// roundUp takes an int64 greater than 0 and rounds it up to the next
// power of 2.
func nextPow2(v int64) int64 {
	// normalizes if already power of 2. when input is 8, we want to return 8, not 16
	v-- // 1. make it easier to handle exact powers of 2
	// fill in the rest of the bytes with 1s
	v |= v >> 1 // 2. propagate the highest bit to the right
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32 // after these, all bits below the highest 1 are set
	v++          // 3. adding 1 makes it a power of 2
	return v
}

type RingBuffer[T any] struct {
	// Writer Hot Mutated Fields
	// Isolate mutating fields to their own cache line.
	writeCursor         atomic.Int64 // 8 bytes
	nextSequence        int64        // 8 bytes
	cachedSlowestReader int64        // 8 bytes
	_                   [CacheLineSize - 8 - 8 - 8]byte

	// Read-Only
	// Wait Strategy Configuration
	writerWaitStrategyParams WaitStrategyParams // 24 bytes
	writerWaitStrategy       WaitStrategy       // 1 byte + 7 bytes implicit padding to align bufferSize
	bufferSize               int64              // 8 bytes
	mask                     int64              // 8 bytes
	gatingBarrier            WriterBarrier      // 16 bytes; defaults to &barrier; override for pipeline leaf stage
	_                        [CacheLineSize - 24 - 1 - 7 - 8 - 8 - 16]byte

	buffer []T

	// barrier (Aligned to next cache line)
	barrier BitmapReaderPool[T]
}

// RingBufferOption configures a RingBuffer
type RingBufferOption[T any] func(*RingBuffer[T])

// WithWaitStrategy sets the wait strategy for writer backpressure
func WithWaitStrategy[T any](strategy WaitStrategy) RingBufferOption[T] {
	return func(r *RingBuffer[T]) {
		r.writerWaitStrategy = strategy
	}
}

// WithWaitStrategyParams sets additional parameters for the wait strategy
func WithWaitStrategyParams[T any](params WaitStrategyParams) RingBufferOption[T] {
	return func(r *RingBuffer[T]) {
		r.writerWaitStrategyParams = params
	}
}

func DefaultWriterWaitStrategyParams() WaitStrategyParams {
	return WaitStrategyParams{
		SleepDuration:       time.Microsecond,
		HybridSpinCount:     100,
		HybridSleepDuration: time.Microsecond,
	}
}

func NewRingBuffer[T any](ctx context.Context, capacity int64, opts ...RingBufferOption[T]) (*RingBuffer[T], error) {
	if capacity < 2 {
		return nil, errors.New("capacity must be at least 2")
	}

	capacity = nextPow2(capacity)
	r := &RingBuffer[T]{
		buffer:                   make([]T, capacity),
		bufferSize:               capacity,
		mask:                     capacity - 1,
		barrier:                  BitmapReaderPool[T]{},
		writerWaitStrategy:       WaitStrategyYield,
		writerWaitStrategyParams: DefaultWriterWaitStrategyParams(),
	}

	// Apply options
	for _, opt := range opts {
		opt(r)
	}

	// wire up barrier
	r.barrier.buffer = r.buffer
	r.barrier.mask = r.mask
	r.barrier.rootCtx, r.barrier.rootCancel = context.WithCancel(ctx)
	r.barrier.writerCursor = &r.writeCursor
	r.gatingBarrier = &r.barrier

	// initialize all cursors to writer position
	writerPos := r.barrier.writerCursor.Load()
	for i := range 128 {
		r.barrier.slots[i].cursor.Store(writerPos)
	}

	return r, nil
}

func (r *RingBuffer[T]) Publish(payload T) {
	nextSequence := r.nextSequence + 1
	// hot path
	if nextSequence < r.cachedSlowestReader+r.bufferSize {
		r.buffer[nextSequence&r.mask] = payload
		r.writeCursor.Store(nextSequence)
		r.nextSequence = nextSequence
		return
	}

	// slow path
	var spins uint64
	for nextSequence >= r.cachedSlowestReader+r.bufferSize {
		wait(r.writerWaitStrategy, &r.writerWaitStrategyParams, spins)
		r.cachedSlowestReader = r.gatingBarrier.Load()
		spins++
	}
	r.buffer[nextSequence&r.mask] = payload
	r.writeCursor.Store(nextSequence)
	r.nextSequence = nextSequence
}

func (r *RingBuffer[T]) PublishFunc(f func(*T)) {
	nextSequence := r.nextSequence + 1
	// hot path
	if nextSequence < r.cachedSlowestReader+r.bufferSize {
		f(&r.buffer[nextSequence&r.mask])
		r.writeCursor.Store(nextSequence)
		r.nextSequence = nextSequence
		return
	}

	// slow path
	var spins uint64
	for nextSequence >= r.cachedSlowestReader+r.bufferSize {
		wait(r.writerWaitStrategy, &r.writerWaitStrategyParams, spins)
		r.cachedSlowestReader = r.gatingBarrier.Load()
		spins++
	}
	f(&r.buffer[nextSequence&r.mask])
	r.writeCursor.Store(nextSequence)
	r.nextSequence = nextSequence
}

// TryPublish attempts to publish without blocking. It returns false when the
// ring is full, after refreshing the gating barrier once. Like Publish, it
// must only be called from the writer goroutine.
func (r *RingBuffer[T]) TryPublish(payload T) bool {
	nextSequence := r.nextSequence + 1
	if nextSequence >= r.cachedSlowestReader+r.bufferSize {
		// one refresh, then give up instead of waiting
		r.cachedSlowestReader = r.gatingBarrier.Load()
		if nextSequence >= r.cachedSlowestReader+r.bufferSize {
			return false
		}
	}
	r.buffer[nextSequence&r.mask] = payload
	r.writeCursor.Store(nextSequence)
	r.nextSequence = nextSequence
	return true
}

// TryPublishFunc is the non-blocking sibling of PublishFunc. It returns false
// when the ring is full, in which case f is not called. Like PublishFunc, it
// must only be called from the writer goroutine.
func (r *RingBuffer[T]) TryPublishFunc(f func(*T)) bool {
	nextSequence := r.nextSequence + 1
	if nextSequence >= r.cachedSlowestReader+r.bufferSize {
		// one refresh, then give up instead of waiting
		r.cachedSlowestReader = r.gatingBarrier.Load()
		if nextSequence >= r.cachedSlowestReader+r.bufferSize {
			return false
		}
	}
	f(&r.buffer[nextSequence&r.mask])
	r.writeCursor.Store(nextSequence)
	r.nextSequence = nextSequence
	return true
}

// segments returns the backing-buffer slices spanning sequences
// firstSeq..firstSeq+n-1. seg2 is non-nil only when the range wraps the ring
// end. Callers must have already established that the range is claimable.
func (r *RingBuffer[T]) segments(firstSeq, n int64) (seg1, seg2 []T) {
	start := firstSeq & r.mask
	end := start + n
	if end <= r.bufferSize {
		return r.buffer[start:end], nil
	}
	seg1 = r.buffer[start:r.bufferSize]
	seg2 = r.buffer[0 : n-int64(len(seg1))]
	return seg1, seg2
}

// Reserve blocks until n contiguous sequence numbers are available and returns
// up to two slices into the backing buffer spanning those slots. seg2 is non-nil
// only when the reservation wraps the end of the ring. The caller must fill every
// slot in both segments and then pass the returned claim to Commit exactly once,
// in publish order. n must satisfy 0 < n < bufferSize.
//
// Callers compiled with GOEXPERIMENT=simd may fill seg1 and seg2 using the simd
// package's aligned loads/stores; each segment is contiguous in memory.
func (r *RingBuffer[T]) Reserve(n int64) (seg1, seg2 []T, claim int64) {
	if n <= 0 || n >= r.bufferSize {
		panic("ringring: Reserve n out of range (must satisfy 0 < n < bufferSize)")
	}
	firstSeq := r.nextSequence + 1
	lastSeq := r.nextSequence + n

	// slow path: block until the last slot in the batch is free
	if lastSeq >= r.cachedSlowestReader+r.bufferSize {
		var spins uint64
		for lastSeq >= r.cachedSlowestReader+r.bufferSize {
			wait(r.writerWaitStrategy, &r.writerWaitStrategyParams, spins)
			r.cachedSlowestReader = r.gatingBarrier.Load()
			spins++
		}
	}

	seg1, seg2 = r.segments(firstSeq, n)
	return seg1, seg2, lastSeq
}

// Commit publishes a batch previously returned by Reserve. It must be called
// exactly once per Reserve, with the claim returned by that Reserve, and in the
// same order as the Reserve calls. A single atomic Store makes the entire batch
// visible to readers.
func (r *RingBuffer[T]) Commit(claim int64) {
	r.writeCursor.Store(claim)
	r.nextSequence = claim
}

// TryReserve is the non-blocking sibling of Reserve. When n contiguous slots
// are free it behaves exactly like Reserve and returns ok = true; the caller
// must then pass claim to Commit exactly once, in publish order. When the ring
// lacks room for the whole batch it returns ok = false (after refreshing the
// gating barrier once) and no claim is made. Panics if n is out of range,
// matching Reserve. Must only be called from the writer goroutine.
func (r *RingBuffer[T]) TryReserve(n int64) (seg1, seg2 []T, claim int64, ok bool) {
	if n <= 0 || n >= r.bufferSize {
		panic("ringring: TryReserve n out of range (must satisfy 0 < n < bufferSize)")
	}
	lastSeq := r.nextSequence + n
	if lastSeq >= r.cachedSlowestReader+r.bufferSize {
		// one refresh, then give up instead of waiting
		r.cachedSlowestReader = r.gatingBarrier.Load()
		if lastSeq >= r.cachedSlowestReader+r.bufferSize {
			return nil, nil, 0, false
		}
	}
	seg1, seg2 = r.segments(r.nextSequence+1, n)
	return seg1, seg2, lastSeq, true
}

// Remaining reports how many slots the writer can publish right now without
// blocking. It refreshes the gating barrier, so it reports actual capacity
// rather than the writer's cached view; the result is 0 exactly when
// TryPublish would return false. The maximum value is bufferSize-1 (strict <
// gating, consistent with Reserve's n < bufferSize rule). It does not account
// for an uncommitted Reserve claim, so call it between Commit and the next
// Reserve. Must only be called from the writer goroutine.
func (r *RingBuffer[T]) Remaining() int64 {
	r.cachedSlowestReader = r.gatingBarrier.Load()
	return r.cachedSlowestReader + r.bufferSize - 1 - r.nextSequence
}

// PublishBatchFunc reserves n slots and invokes f for each in publish order,
// passing a pointer into the backing buffer. f must not retain the pointer past
// its call. Wrap-around is handled internally; f sees indices 0..n-1.
func (r *RingBuffer[T]) PublishBatchFunc(n int64, f func(i int64, slot *T)) {
	seg1, seg2, claim := r.Reserve(n)
	var i int64
	for j := range seg1 {
		f(i, &seg1[j])
		i++
	}
	for j := range seg2 {
		f(i, &seg2[j])
		i++
	}
	r.Commit(claim)
}

// PublishBatch copies payloads into len(payloads) consecutive slots and
// publishes them atomically. Empty payloads is a no-op.
func (r *RingBuffer[T]) PublishBatch(payloads []T) {
	n := int64(len(payloads))
	if n == 0 {
		return
	}
	seg1, seg2, claim := r.Reserve(n)
	copy(seg1, payloads[:len(seg1)])
	if len(seg2) > 0 {
		copy(seg2, payloads[len(seg1):])
	}
	r.Commit(claim)
}

type WriterBarrier interface {
	Load() int64
}

type ReaderBarrier interface {
	Load() int64
}

type MinimumBarrier []ReaderBarrier

// Load implements a branch-free min comparison to find the slowest reader's position
//
// # Example
//
// Let's say we have 3 readers at different positions:
//
// Reader 0: sequence = 100
// Reader 1: sequence = 95  (slowest)
// Reader 2: sequence = 102
//
// Iteration 1: i=0
// minimum = 100 (initialized from m[0])
//
// Iteration 2: i=1
//
// seq = 95
// diff = minimum - seq = 100 - 95 = 5
//
// ## Binary representation (int64):
//
// diff = 0000...0101 (positive number)
//
// ## Arithmetic right shift by 63:
//
// mask = diff >> 63 = 0000...0000 = 0
//
// ## Update minimum:
//
//	minimum = seq + (diff & mask)
//	= 95 + (5 & 0)
//	= 95 + 0
//	= 95  ✓ (updated to smaller cursor)
//
// Iteration 3: i=2
//
// seq = 102
// diff = minimum - seq = 95 - 102 = -7
//
// ## Binary representation (int64 two's complement):
//
// diff = 1111...1001 (negative number, sign bit = 1)
//
// ## Arithmetic right shift by 63:
//
// mask = diff >> 63 = 1111...1111 = -1 (all bits set)
//
// ## Update minimum:
//
//	minimum = seq + (diff & mask)
//	= 102 + (-7 & -1)
//	= 102 + (-7)
//	= 95  ✓ (kept smaller cursor)
func (m MinimumBarrier) Load() int64 {
	minimum := m[0].Load()
	for i := 1; i < len(m); i++ {
		seq := m[i].Load()
		diff := minimum - seq
		mask := diff >> 63 // arithmetic right shift: 0 if diff >= 0; -1 if diff < 0
		minimum = seq + (diff & mask)
	}

	return minimum
}
