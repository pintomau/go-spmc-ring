package cdv_queue

import (
	"errors"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	// ErrDisposed is returned when an operation is performed on a disposed queue.
	ErrDisposed = errors.New(`queue: disposed`)

	// ErrTimeout is returned when an applicable queue operation times out.
	ErrTimeout = errors.New(`queue: poll timed out`)
)

// roundUp takes an uint64 greater than 0 and rounds it up to the next
// power of 2.
func roundUp(v uint64) uint64 {
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

type node[T interface{}] struct {
	position uint64
	data     T
}

type nodes[T any] []node[T]

type RingBuffer[T any] struct {
	_padding0 [64]byte // force new cache line to prevent false-sharing
	queue     uint64
	_padding1 [64]byte
	dequeue   uint64
	_padding2 [40]byte
	startTime time.Time
	mask      uint64
	disposed  uint64
	_padding3 [64]byte
	nodes     nodes[T]
}

// NewRingBuffer will allocate, initialize, and return a ring buffer
// with the specified size.
func NewRingBuffer[T any](size uint64) *RingBuffer[T] {
	rb := &RingBuffer[T]{}
	rb.init(size)
	return rb
}

func (rb *RingBuffer[T]) init(size uint64) {
	size = roundUp(size)
	rb.nodes = make(nodes[T], size)
	for i := uint64(0); i < size; i++ {
		rb.nodes[i] = node[T]{position: i}
	}
	rb.mask = size - 1 // so we don't have to do this with every put/get operation
	rb.startTime = time.Now()
}

// Put adds the provided item to the queue.  If the queue is full, this
// call will block until an item is added to the queue or Dispose is called
// on the queue.  An error will be returned if the queue is disposed.
func (rb *RingBuffer[T]) Put(item T) error {
	_, err := rb.put(item, false)
	return err
}

// Offer adds the provided item to the queue if there is space.  If the queue
// is full, this call will return false.  An error will be returned if the
// queue is disposed.
func (rb *RingBuffer[T]) Offer(item T) (bool, error) {
	return rb.put(item, true)
}

func (rb *RingBuffer[T]) put(item T, offer bool) (bool, error) {
	var n *node[T]
	pos := atomic.LoadUint64(&rb.queue)
L:
	for {
		if atomic.LoadUint64(&rb.disposed) == 1 {
			return false, ErrDisposed
		}

		// When size is a power of 2, mask = size - 1 creates a bitmask:
		//
		//  size = 8       →  binary: 00001000
		//  mask = 7       →  binary: 00000111
		//
		//  pos = 0   →  0 & 7 = 0    (00000000 & 00000111 = 00000000)
		//  pos = 5   →  5 & 7 = 5    (00000101 & 00000111 = 00000101)
		//  pos = 8   →  8 & 7 = 0    (00001000 & 00000111 = 00000000)  ← wraps!
		//  pos = 13  → 13 & 7 = 5    (00001101 & 00000111 = 00000101)
		n = &rb.nodes[pos&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		switch dif := seq - pos; {
		case dif == 0:
			// The node is ready to be written. The sequence number matches the expected position and generation,
			// meaning this slot is available. The CAS operation attempts to claim it.
			if atomic.CompareAndSwapUint64(&rb.queue, pos, pos+1) {
				// Exit both switch and loop
				break L
			}
		case dif < 0:
			panic(`Ring buffer in a compromised state during a put operation.`)
		default:
			// The node is not yet available. Another goroutine is still writing to it, or a consumer hasn't read
			// it yet. The code reloads pos to get the updated queue position and retries.
			pos = atomic.LoadUint64(&rb.queue)
		}

		if offer {
			return false, nil
		}

		runtime.Gosched() // free up the cpu before the next iteration
	}

	// Since we were able to claim the position in the queue
	// Let's update the node with the position as well
	n.data = item
	atomic.StoreUint64(&n.position, pos+1)
	return true, nil
}

// Get will return the next item in the queue.  This call will block
// if the queue is empty.  This call will unblock when an item is added
// to the queue or Dispose is called on the queue.  An error will be returned
// if the queue is disposed.
func (rb *RingBuffer[T]) Get() (T, error) {
	return rb.Poll(0)
}

// Poll will return the next item in the queue.  This call will block
// if the queue is empty.  This call will unblock when an item is added
// to the queue, Dispose is called on the queue, or the timeout is reached. An
// error will be returned if the queue is disposed or a timeout occurs. A
// non-positive timeout will block indefinitely.
func (rb *RingBuffer[T]) Poll(timeoutDelta time.Duration) (T, error) {
	var (
		n       *node[T]
		pos     = atomic.LoadUint64(&rb.dequeue)
		timeout time.Duration
		zeroT   T
	)

	if timeoutDelta > 0 {
		timeout = time.Since(rb.startTime) + timeoutDelta
	}
L:
	for {
		if atomic.LoadUint64(&rb.disposed) == 1 {
			return zeroT, ErrDisposed
		}

		// When size is a power of 2, mask = size - 1 creates a bitmask:
		//
		//  size = 8       →  binary: 00001000
		//  mask = 7       →  binary: 00000111
		//
		//  pos = 0   →  0 & 7 = 0    (00000000 & 00000111 = 00000000)
		//  pos = 5   →  5 & 7 = 5    (00000101 & 00000111 = 00000101)
		//  pos = 8   →  8 & 7 = 0    (00001000 & 00000111 = 00000000)  ← wraps!
		//  pos = 13  → 13 & 7 = 5    (00001101 & 00000111 = 00000101)
		n = &rb.nodes[pos&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		switch dif := seq - (pos + 1); {
		case dif == 0:
			// The node is ready to be written. The sequence number matches the expected position and generation.
			if atomic.CompareAndSwapUint64(&rb.dequeue, pos, pos+1) {
				// Exit both switch and loop
				break L
			}
		case dif < 0:
			panic(`Ring buffer in compromised state during a get operation.`)
		default:
			// The node is not yet available.
			pos = atomic.LoadUint64(&rb.dequeue)
		}

		if timeoutDelta > 0 && time.Since(rb.startTime) >= timeout {
			return zeroT, ErrTimeout
		}

		runtime.Gosched() // free up the cpu before the next iteration
	}

	data := n.data
	n.data = zeroT
	// pos + mask + 1 = pos + size -> advance to the next lap at this index
	atomic.StoreUint64(&n.position, pos+rb.mask+1)
	return data, nil
}
