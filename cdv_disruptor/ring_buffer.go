package cdv_disruptor

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
	_                        [CacheLineSize - 24 - 1 - 7 - 8 - 8]byte

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
		r.cachedSlowestReader = r.barrier.Load()
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
		r.cachedSlowestReader = r.barrier.Load()
		spins++
	}
	f(&r.buffer[nextSequence&r.mask])
	r.writeCursor.Store(nextSequence)
	r.nextSequence = nextSequence
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
