package cdv_queue

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// Disruptor implements a single-producer, multi-consumer ring buffer
// where multiple readers can independently read the same data.
// Based on the LMAX Disruptor pattern.
type Disruptor[T any] struct {
	// Producer cursor - where we're writing
	_padding0 [64]byte
	cursor    uint64
	_padding1 [64]byte

	// buffer metadata
	mask       uint64
	bufferSize uint64

	// Reader management
	readersMu sync.RWMutex
	readers   []*Reader[T]

	_padding2 [64]byte

	// Ring buffer storage
	buffer []T
}

// NewDisruptor creates a new Disruptor with the given buffer size.
// Size will be rounded up to the nearest power of 2.
func NewDisruptor[T any](size uint64) *Disruptor[T] {
	size = roundUp(size)
	return &Disruptor[T]{
		cursor:     0,
		mask:       size - 1,
		bufferSize: size,
		buffer:     make([]T, size),
		readers:    make([]*Reader[T], 0, 4),
	}
}

// NewReader creates and registers a new reader starting from the current position.
// The reader will see all events published after this point.
func (d *Disruptor[T]) NewReader() *Reader[T] {
	d.readersMu.Lock()
	defer d.readersMu.Unlock()

	reader := &Reader[T]{
		sequence:  atomic.LoadUint64(&d.cursor),
		disruptor: d,
		id:        len(d.readers),
	}
	d.readers = append(d.readers, reader)
	return reader
}

// NewReaderFrom creates a reader starting from a specific sequence.
// Useful for replaying events from a known position.
func (d *Disruptor[T]) NewReaderFrom(seq uint64) *Reader[T] {
	d.readersMu.Lock()
	defer d.readersMu.Unlock()

	reader := &Reader[T]{
		sequence:  seq,
		disruptor: d,
		id:        len(d.readers),
	}
	d.readers = append(d.readers, reader)
	return reader
}

// RemoveReader unregisters a reader from the Disruptor.
// After removal, the reader should not be used.
func (d *Disruptor[T]) RemoveReader(r *Reader[T]) {
	d.readersMu.Lock()
	defer d.readersMu.Unlock()

	for i, reader := range d.readers {
		if reader == r {
			// Remove by swapping with last element
			d.readers[i] = d.readers[len(d.readers)-1]
			d.readers[len(d.readers)-1] = nil
			d.readers = d.readers[:len(d.readers)-1]
			break
		}
	}
}

// Publish writes an event to the ring buffer.
// Blocks if the slowest reader hasn't caught up (would overwrite unread data).
func (d *Disruptor[T]) Publish(item T) {
	next := atomic.LoadUint64(&d.cursor) + 1

	// Wait if we would overwrite data not yet read by all readers
	for {
		minSeq := d.minReaderSequence()
		// Check if next position would wrap around and overwrite unread data
		if next-minSeq <= d.bufferSize {
			break
		}
		runtime.Gosched()
	}

	// Write the data
	d.buffer[next&d.mask] = item

	// Publish by advancing cursor (memory barrier)
	atomic.StoreUint64(&d.cursor, next)
}

// TryPublish attempts to write an event without blocking.
// Returns false if the buffer is full (slowest reader hasn't caught up).
func (d *Disruptor[T]) TryPublish(item T) bool {
	next := atomic.LoadUint64(&d.cursor) + 1
	minSeq := d.minReaderSequence()

	// Would overwrite unread data
	if next-minSeq > d.bufferSize {
		return false
	}

	d.buffer[next&d.mask] = item
	atomic.StoreUint64(&d.cursor, next)
	return true
}

// Cursor returns the current producer position.
func (d *Disruptor[T]) Cursor() uint64 {
	return atomic.LoadUint64(&d.cursor)
}

// minReaderSequence returns the sequence of the slowest reader.
// If no readers, returns cursor (no backpressure).
func (d *Disruptor[T]) minReaderSequence() uint64 {
	d.readersMu.RLock()
	defer d.readersMu.RUnlock()

	if len(d.readers) == 0 {
		return atomic.LoadUint64(&d.cursor)
	}

	min := atomic.LoadUint64(&d.readers[0].sequence)
	for i := 1; i < len(d.readers); i++ {
		seq := atomic.LoadUint64(&d.readers[i].sequence)
		if seq < min {
			min = seq
		}
	}
	return min
}

// Reader represents an independent consumer that can read events
// from the Disruptor without affecting other readers.
type Reader[T any] struct {
	_padding0 [64]byte
	sequence  uint64 // This reader's current position
	_padding1 [64]byte

	disruptor *Disruptor[T]
	id        int
}

// Next blocks until the next event is available and returns it.
// Uses yielding wait strategy: spins briefly then yields CPU.
func (r *Reader[T]) Next() (T, uint64) {
	next := atomic.LoadUint64(&r.sequence) + 1

	// Yielding wait strategy
	counter := 100
	for atomic.LoadUint64(&r.disruptor.cursor) < next {
		if counter > 0 {
			counter--
			continue // spin
		}
		runtime.Gosched() // yield
		counter = 100     // reset spin counter after yield
	}

	// Read the data
	item := r.disruptor.buffer[next&r.disruptor.mask]

	// Advance our sequence
	atomic.StoreUint64(&r.sequence, next)

	return item, next
}

// TryNext attempts to read the next event without blocking.
// Returns the event, sequence, and true if available; zero value and false otherwise.
func (r *Reader[T]) TryNext() (T, uint64, bool) {
	next := atomic.LoadUint64(&r.sequence) + 1

	// Check if data is available
	if atomic.LoadUint64(&r.disruptor.cursor) < next {
		var zero T
		return zero, 0, false
	}

	// Read the data
	item := r.disruptor.buffer[next&r.disruptor.mask]

	// Advance our sequence
	atomic.StoreUint64(&r.sequence, next)

	return item, next, true
}

// Sequence returns this reader's current position.
func (r *Reader[T]) Sequence() uint64 {
	return atomic.LoadUint64(&r.sequence)
}

// Available returns the number of events available for this reader.
func (r *Reader[T]) Available() uint64 {
	cursor := atomic.LoadUint64(&r.disruptor.cursor)
	seq := atomic.LoadUint64(&r.sequence)
	if cursor > seq {
		return cursor - seq
	}
	return 0
}

// BatchNext reads up to n events in a batch for better throughput.
// Returns the slice of events read and their ending sequence.
func (r *Reader[T]) BatchNext(n int) ([]T, uint64) {
	if n <= 0 {
		return nil, r.Sequence()
	}

	start := atomic.LoadUint64(&r.sequence) + 1

	// Wait for at least one event
	counter := 100
	for atomic.LoadUint64(&r.disruptor.cursor) < start {
		if counter > 0 {
			counter--
			continue
		}
		runtime.Gosched()
		counter = 100
	}

	// Determine how many we can read
	cursor := atomic.LoadUint64(&r.disruptor.cursor)
	available := int(cursor - start + 1)
	if available > n {
		available = n
	}

	// Read the batch
	items := make([]T, available)
	for i := 0; i < available; i++ {
		seq := start + uint64(i)
		items[i] = r.disruptor.buffer[seq&r.disruptor.mask]
	}

	// Advance sequence
	end := start + uint64(available) - 1
	atomic.StoreUint64(&r.sequence, end)

	return items, end
}
