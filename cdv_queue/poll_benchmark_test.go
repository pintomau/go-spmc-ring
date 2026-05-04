package cdv_queue

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Implementation 1: Using time.Since(startTime) - CURRENT
// =============================================================================

type RingBufferSince[T any] struct {
	_padding0 [64]byte
	queue     uint64
	_padding1 [64]byte
	dequeue   uint64
	_padding2 [40]byte    // 40 bytes padding
	startTime time.Time   // 24 bytes - together = 64 bytes cache line
	mask      uint64
	disposed  uint64
	_padding3 [64]byte
	nodes     nodes[T]
}

func NewRingBufferSince[T any](size uint64) *RingBufferSince[T] {
	rb := &RingBufferSince[T]{}
	rb.init(size)
	return rb
}

func (rb *RingBufferSince[T]) init(size uint64) {
	size = roundUp(size)
	rb.nodes = make(nodes[T], size)
	for i := uint64(0); i < size; i++ {
		rb.nodes[i] = node[T]{position: i}
	}
	rb.mask = size - 1
	rb.startTime = time.Now()
}

func (rb *RingBufferSince[T]) Put(item T) error {
	var n *node[T]
	pos := atomic.LoadUint64(&rb.queue)
L:
	for {
		if atomic.LoadUint64(&rb.disposed) == 1 {
			return ErrDisposed
		}
		n = &rb.nodes[pos&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		switch dif := seq - pos; {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&rb.queue, pos, pos+1) {
				break L
			}
		case dif < 0:
			panic(`Ring buffer in compromised state during a put operation.`)
		default:
			pos = atomic.LoadUint64(&rb.queue)
		}
		runtime.Gosched()
	}
	n.data = item
	atomic.StoreUint64(&n.position, pos+1)
	return nil
}

func (rb *RingBufferSince[T]) Poll(timeoutDelta time.Duration) (T, error) {
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

		n = &rb.nodes[pos&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		switch dif := seq - (pos + 1); {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&rb.dequeue, pos, pos+1) {
				break L
			}
		case dif < 0:
			panic(`Ring buffer in compromised state during a get operation.`)
		default:
			pos = atomic.LoadUint64(&rb.dequeue)
		}

		if timeoutDelta > 0 && time.Since(rb.startTime) >= timeout {
			return zeroT, ErrTimeout
		}

		runtime.Gosched()
	}
	data := n.data
	n.data = zeroT
	atomic.StoreUint64(&n.position, pos+rb.mask+1)
	return data, nil
}

// =============================================================================
// Implementation 2: Using time.Now() directly - ALTERNATIVE
// =============================================================================

type RingBufferNow[T any] struct {
	_padding0 [64]byte
	queue     uint64
	_padding1 [64]byte
	dequeue   uint64
	_padding2 [64]byte // Full 64 bytes - no startTime needed
	mask      uint64
	disposed  uint64
	_padding3 [64]byte
	nodes     nodes[T]
}

func NewRingBufferNow[T any](size uint64) *RingBufferNow[T] {
	rb := &RingBufferNow[T]{}
	rb.init(size)
	return rb
}

func (rb *RingBufferNow[T]) init(size uint64) {
	size = roundUp(size)
	rb.nodes = make(nodes[T], size)
	for i := uint64(0); i < size; i++ {
		rb.nodes[i] = node[T]{position: i}
	}
	rb.mask = size - 1
	// No startTime initialization needed
}

func (rb *RingBufferNow[T]) Put(item T) error {
	var n *node[T]
	pos := atomic.LoadUint64(&rb.queue)
L:
	for {
		if atomic.LoadUint64(&rb.disposed) == 1 {
			return ErrDisposed
		}
		n = &rb.nodes[pos&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		switch dif := seq - pos; {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&rb.queue, pos, pos+1) {
				break L
			}
		case dif < 0:
			panic(`Ring buffer in compromised state during a put operation.`)
		default:
			pos = atomic.LoadUint64(&rb.queue)
		}
		runtime.Gosched()
	}
	n.data = item
	atomic.StoreUint64(&n.position, pos+1)
	return nil
}

func (rb *RingBufferNow[T]) Poll(timeoutDelta time.Duration) (T, error) {
	var (
		n        *node[T]
		pos      = atomic.LoadUint64(&rb.dequeue)
		deadline time.Time
		zeroT    T
	)

	if timeoutDelta > 0 {
		deadline = time.Now().Add(timeoutDelta)
	}
L:
	for {
		if atomic.LoadUint64(&rb.disposed) == 1 {
			return zeroT, ErrDisposed
		}

		n = &rb.nodes[pos&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		switch dif := seq - (pos + 1); {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&rb.dequeue, pos, pos+1) {
				break L
			}
		case dif < 0:
			panic(`Ring buffer in compromised state during a get operation.`)
		default:
			pos = atomic.LoadUint64(&rb.dequeue)
		}

		if timeoutDelta > 0 && time.Now().After(deadline) {
			return zeroT, ErrTimeout
		}

		runtime.Gosched()
	}
	data := n.data
	n.data = zeroT
	atomic.StoreUint64(&n.position, pos+rb.mask+1)
	return data, nil
}

// =============================================================================
// Benchmarks
// =============================================================================

const benchSize = 1024

// Benchmark Poll with data immediately available (no timeout path)
func BenchmarkPollImmediate_Since(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Put(i)
		rb.Poll(0)
	}
}

func BenchmarkPollImmediate_Now(b *testing.B) {
	rb := NewRingBufferNow[int](benchSize)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Put(i)
		rb.Poll(0)
	}
}

// Benchmark Poll with timeout calculation (timeout > 0, but data available)
func BenchmarkPollWithTimeout_Since(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Put(i)
		rb.Poll(time.Second)
	}
}

func BenchmarkPollWithTimeout_Now(b *testing.B) {
	rb := NewRingBufferNow[int](benchSize)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Put(i)
		rb.Poll(time.Second)
	}
}

// Benchmark concurrent Put/Poll
func BenchmarkConcurrentPutPoll_Since(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize)
	
	// Producer
	done := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-done:
				return
			default:
				rb.Put(i)
				i++
			}
		}
	}()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Poll(time.Millisecond)
	}
	b.StopTimer()
	close(done)
}

func BenchmarkConcurrentPutPoll_Now(b *testing.B) {
	rb := NewRingBufferNow[int](benchSize)
	
	// Producer
	done := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-done:
				return
			default:
				rb.Put(i)
				i++
			}
		}
	}()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Poll(time.Millisecond)
	}
	b.StopTimer()
	close(done)
}

// Benchmark parallel Put+Poll (multiple goroutines doing both)
func BenchmarkParallelPutPoll_Since(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize * 100)
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rb.Put(i)
			rb.Poll(0)
			i++
		}
	})
}

func BenchmarkParallelPutPoll_Now(b *testing.B) {
	rb := NewRingBufferNow[int](benchSize * 100)
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rb.Put(i)
			rb.Poll(0)
			i++
		}
	})
}

// Benchmark parallel with timeout
func BenchmarkParallelPutPollTimeout_Since(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize * 100)
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rb.Put(i)
			rb.Poll(time.Second)
			i++
		}
	})
}

func BenchmarkParallelPutPollTimeout_Now(b *testing.B) {
	rb := NewRingBufferNow[int](benchSize * 100)
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rb.Put(i)
			rb.Poll(time.Second)
			i++
		}
	})
}

// Benchmark just the time operations in isolation
func BenchmarkTimeOperation_Since(b *testing.B) {
	start := time.Now()
	timeout := time.Second
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = time.Since(start) + timeout
		_ = time.Since(start) >= timeout
	}
}

func BenchmarkTimeOperation_Now(b *testing.B) {
	timeout := time.Second
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deadline := time.Now().Add(timeout)
		_ = time.Now().After(deadline)
	}
}

// =============================================================================
// Vars vs Struct comparison
// =============================================================================

// Poll using multiple vars (original approach)
func (rb *RingBufferSince[T]) PollVars(timeoutDelta time.Duration) (T, error) {
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

		n = &rb.nodes[pos&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		switch dif := seq - (pos + 1); {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&rb.dequeue, pos, pos+1) {
				break L
			}
		case dif < 0:
			panic(`Ring buffer in compromised state during a get operation.`)
		default:
			pos = atomic.LoadUint64(&rb.dequeue)
		}

		if timeoutDelta > 0 && time.Since(rb.startTime) >= timeout {
			return zeroT, ErrTimeout
		}

		runtime.Gosched()
	}
	data := n.data
	n.data = zeroT
	atomic.StoreUint64(&n.position, pos+rb.mask+1)
	return data, nil
}

// Poll using struct (new approach)
func (rb *RingBufferSince[T]) PollStruct(timeoutDelta time.Duration) (T, error) {
	m := struct {
		pos     uint64
		timeout time.Duration
		n       *node[T]
		zeroT   T
	}{
		pos: atomic.LoadUint64(&rb.dequeue),
	}

	if timeoutDelta > 0 {
		m.timeout = time.Since(rb.startTime) + timeoutDelta
	}
L:
	for {
		if atomic.LoadUint64(&rb.disposed) == 1 {
			return m.zeroT, ErrDisposed
		}

		m.n = &rb.nodes[m.pos&rb.mask]
		seq := atomic.LoadUint64(&m.n.position)
		switch dif := seq - (m.pos + 1); {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&rb.dequeue, m.pos, m.pos+1) {
				break L
			}
		case dif < 0:
			panic(`Ring buffer in compromised state during a get operation.`)
		default:
			m.pos = atomic.LoadUint64(&rb.dequeue)
		}

		if timeoutDelta > 0 && time.Since(rb.startTime) >= m.timeout {
			return m.zeroT, ErrTimeout
		}

		runtime.Gosched()
	}
	data := m.n.data
	m.n.data = m.zeroT
	atomic.StoreUint64(&m.n.position, m.pos+rb.mask+1)
	return data, nil
}

// Benchmarks: Vars vs Struct
func BenchmarkPollVars(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Put(i)
		rb.PollVars(0)
	}
}

func BenchmarkPollStruct(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Put(i)
		rb.PollStruct(0)
	}
}

func BenchmarkPollVarsTimeout(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Put(i)
		rb.PollVars(time.Second)
	}
}

func BenchmarkPollStructTimeout(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Put(i)
		rb.PollStruct(time.Second)
	}
}

func BenchmarkParallelPollVars(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize * 100)
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rb.Put(i)
			rb.PollVars(time.Second)
			i++
		}
	})
}

func BenchmarkParallelPollStruct(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize * 100)
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rb.Put(i)
			rb.PollStruct(time.Second)
			i++
		}
	})
}

// =============================================================================
// Cache-stress benchmarks (multiple instances to test false sharing)
// =============================================================================

// Test multiple ring buffers to stress cache lines
func BenchmarkMultiInstance_Since(b *testing.B) {
	const numBuffers = 8
	buffers := make([]*RingBufferSince[int], numBuffers)
	for i := range buffers {
		buffers[i] = NewRingBufferSince[int](benchSize)
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			idx := i % numBuffers
			buffers[idx].Put(i)
			buffers[idx].Poll(time.Millisecond)
			i++
		}
	})
}

func BenchmarkMultiInstance_Now(b *testing.B) {
	const numBuffers = 8
	buffers := make([]*RingBufferNow[int], numBuffers)
	for i := range buffers {
		buffers[i] = NewRingBufferNow[int](benchSize)
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			idx := i % numBuffers
			buffers[idx].Put(i)
			buffers[idx].Poll(time.Millisecond)
			i++
		}
	})
}

// Benchmark to measure memory access patterns
func BenchmarkCacheLineAccess_Since(b *testing.B) {
	rb := NewRingBufferSince[int](benchSize)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Access pattern that touches startTime (same cache line as dequeue)
		rb.Put(i)
		rb.Poll(time.Microsecond * 100)
	}
}

func BenchmarkCacheLineAccess_Now(b *testing.B) {
	rb := NewRingBufferNow[int](benchSize)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Access pattern without startTime field
		rb.Put(i)
		rb.Poll(time.Microsecond * 100)
	}
}

// =============================================================================
// Struct size verification
// =============================================================================

func TestStructLayout(t *testing.T) {
	t.Log("RingBufferSince layout:")
	t.Log("  _padding0: 64 bytes")
	t.Log("  queue:     8 bytes")
	t.Log("  _padding1: 64 bytes")
	t.Log("  dequeue:   8 bytes")
	t.Log("  _padding2: 40 bytes")
	t.Log("  startTime: 24 bytes (time.Time)")
	t.Log("  mask:      8 bytes")
	t.Log("  disposed:  8 bytes")
	t.Log("  _padding3: 64 bytes")
	t.Log("  nodes:     24 bytes (slice header)")
	t.Log("")
	t.Log("RingBufferNow layout:")
	t.Log("  _padding0: 64 bytes")
	t.Log("  queue:     8 bytes")
	t.Log("  _padding1: 64 bytes")
	t.Log("  dequeue:   8 bytes")
	t.Log("  _padding2: 64 bytes (no startTime)")
	t.Log("  mask:      8 bytes")
	t.Log("  disposed:  8 bytes")
	t.Log("  _padding3: 64 bytes")
	t.Log("  nodes:     24 bytes (slice header)")
	t.Log("")
	t.Log("Key difference:")
	t.Log("  Since: time.Since(startTime) - reads cached start, computes duration")
	t.Log("  Now:   time.Now().Add/After - calls time.Now() twice per timeout check")
}
