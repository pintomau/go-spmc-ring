package cdv_queue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDisruptorBasic(t *testing.T) {
	d := NewDisruptor[int](8)
	reader := d.NewReader()

	// Publish some events
	for i := 1; i <= 5; i++ {
		d.Publish(i)
	}

	// Read them back
	for i := 1; i <= 5; i++ {
		val, seq := reader.Next()
		if val != i {
			t.Errorf("Expected %d, got %d", i, val)
		}
		if seq != uint64(i) {
			t.Errorf("Expected seq %d, got %d", i, seq)
		}
	}
}

func TestDisruptorMultipleReaders(t *testing.T) {
	d := NewDisruptor[int](16)
	reader1 := d.NewReader()
	reader2 := d.NewReader()

	// Publish events
	for i := 1; i <= 10; i++ {
		d.Publish(i)
	}

	// Both readers should see all events
	for i := 1; i <= 10; i++ {
		val1, _ := reader1.Next()
		val2, _ := reader2.Next()

		if val1 != i {
			t.Errorf("Reader1: expected %d, got %d", i, val1)
		}
		if val2 != i {
			t.Errorf("Reader2: expected %d, got %d", i, val2)
		}
	}
}

func TestDisruptorTryNext(t *testing.T) {
	d := NewDisruptor[int](8)
	reader := d.NewReader()

	// TryNext should fail when empty
	_, _, ok := reader.TryNext()
	if ok {
		t.Error("TryNext should return false when no data available")
	}

	// Publish and TryNext should succeed
	d.Publish(42)
	val, seq, ok := reader.TryNext()
	if !ok {
		t.Error("TryNext should return true when data available")
	}
	if val != 42 || seq != 1 {
		t.Errorf("Expected 42 at seq 1, got %d at seq %d", val, seq)
	}
}

func TestDisruptorTryPublish(t *testing.T) {
	d := NewDisruptor[int](4)
	reader := d.NewReader()

	// Should succeed up to buffer size
	for i := 1; i <= 4; i++ {
		if !d.TryPublish(i) {
			t.Errorf("TryPublish should succeed for item %d", i)
		}
	}

	// Should fail when buffer is full (reader hasn't caught up)
	if d.TryPublish(5) {
		t.Error("TryPublish should fail when buffer is full")
	}

	// Read one, should be able to publish again
	reader.Next()
	if !d.TryPublish(5) {
		t.Error("TryPublish should succeed after reader advances")
	}
}

func TestDisruptorBatchNext(t *testing.T) {
	d := NewDisruptor[int](16)
	reader := d.NewReader()

	// Publish 10 events
	for i := 1; i <= 10; i++ {
		d.Publish(i)
	}

	// Read in batch of 4
	items, seq := reader.BatchNext(4)
	if len(items) != 4 {
		t.Errorf("Expected 4 items, got %d", len(items))
	}
	if seq != 4 {
		t.Errorf("Expected end seq 4, got %d", seq)
	}
	for i, v := range items {
		if v != i+1 {
			t.Errorf("Expected %d at index %d, got %d", i+1, i, v)
		}
	}
}

func TestDisruptorConcurrentReaders(t *testing.T) {
	d := NewDisruptor[int](1024)
	numReaders := 4
	numEvents := 1000

	readers := make([]*Reader[int], numReaders)
	for i := 0; i < numReaders; i++ {
		readers[i] = d.NewReader()
	}

	// Start readers
	var wg sync.WaitGroup
	results := make([][]int, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = make([]int, numEvents)
			for j := 0; j < numEvents; j++ {
				val, _ := readers[idx].Next()
				results[idx][j] = val
			}
		}(i)
	}

	// Publish events
	for i := 0; i < numEvents; i++ {
		d.Publish(i)
	}

	wg.Wait()

	// Verify all readers got all events in order
	for i := 0; i < numReaders; i++ {
		for j := 0; j < numEvents; j++ {
			if results[i][j] != j {
				t.Errorf("Reader %d: expected %d at index %d, got %d", i, j, j, results[i][j])
			}
		}
	}
}

func TestDisruptorRemoveReader(t *testing.T) {
	d := NewDisruptor[int](8)
	reader1 := d.NewReader()
	reader2 := d.NewReader()

	// Publish some events
	for i := 1; i <= 4; i++ {
		d.Publish(i)
	}

	// Reader1 reads but reader2 doesn't
	for i := 1; i <= 4; i++ {
		reader1.Next()
	}

	// Remove the slow reader
	d.RemoveReader(reader2)

	// Now should be able to publish more (no slow reader blocking)
	for i := 5; i <= 8; i++ {
		if !d.TryPublish(i) {
			t.Errorf("Should be able to publish after removing slow reader")
		}
	}
}

func TestDisruptorAvailable(t *testing.T) {
	d := NewDisruptor[int](16)
	reader := d.NewReader()

	if reader.Available() != 0 {
		t.Errorf("Expected 0 available, got %d", reader.Available())
	}

	d.Publish(1)
	d.Publish(2)
	d.Publish(3)

	if reader.Available() != 3 {
		t.Errorf("Expected 3 available, got %d", reader.Available())
	}

	reader.Next()
	if reader.Available() != 2 {
		t.Errorf("Expected 2 available, got %d", reader.Available())
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

const disruptorBenchSize = 1024

// Single producer, single reader
func BenchmarkDisruptorSingleReader(b *testing.B) {
	d := NewDisruptor[int](disruptorBenchSize)
	reader := d.NewReader()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Publish(i)
		reader.Next()
	}
}

// Single producer, multiple readers (broadcast)
func BenchmarkDisruptorMultiReader2(b *testing.B) {
	d := NewDisruptor[int](disruptorBenchSize)
	reader1 := d.NewReader()
	reader2 := d.NewReader()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Publish(i)
		reader1.Next()
		reader2.Next()
	}
}

func BenchmarkDisruptorMultiReader4(b *testing.B) {
	d := NewDisruptor[int](disruptorBenchSize)
	readers := make([]*Reader[int], 4)
	for i := range readers {
		readers[i] = d.NewReader()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Publish(i)
		for _, r := range readers {
			r.Next()
		}
	}
}

// Compare with RingBuffer (queue semantics - consume once)
func BenchmarkRingBufferSingleConsumer(b *testing.B) {
	rb := NewRingBuffer[int](disruptorBenchSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Put(i)
		rb.Poll(0)
	}
}

// Concurrent benchmark
func BenchmarkDisruptorConcurrent(b *testing.B) {
	d := NewDisruptor[int](disruptorBenchSize * 10)
	reader := d.NewReader()

	// Producer goroutine
	done := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-done:
				return
			default:
				d.Publish(i)
				i++
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Next()
	}
	b.StopTimer()
	close(done)
}

// Batch reading benchmark
func BenchmarkDisruptorBatchRead(b *testing.B) {
	d := NewDisruptor[int](disruptorBenchSize * 10)
	reader := d.NewReader()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Publish a batch
		for j := 0; j < 10; j++ {
			d.Publish(i*10 + j)
		}
		// Read as batch
		reader.BatchNext(10)
	}
}

// Parallel readers benchmark
func BenchmarkDisruptorParallelReaders(b *testing.B) {
	d := NewDisruptor[int](disruptorBenchSize * 100)

	// Start publishing
	var published uint64
	done := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-done:
				return
			default:
				d.Publish(i)
				atomic.AddUint64(&published, 1)
				i++
			}
		}
	}()

	// Wait for some data
	time.Sleep(10 * time.Millisecond)

	b.RunParallel(func(pb *testing.PB) {
		reader := d.NewReader()
		for pb.Next() {
			reader.Next()
		}
		d.RemoveReader(reader)
	})

	close(done)
}

// TryNext benchmark (non-blocking)
func BenchmarkDisruptorTryNext(b *testing.B) {
	d := NewDisruptor[int](disruptorBenchSize)
	reader := d.NewReader()

	// Pre-fill
	for i := 0; i < disruptorBenchSize; i++ {
		d.Publish(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := reader.TryNext(); !ok {
			// Refill when empty
			for j := 0; j < disruptorBenchSize; j++ {
				d.Publish(j)
			}
		}
	}
}

// Compare: Disruptor broadcast vs multiple RingBuffer consumers
func BenchmarkCompare_DisruptorBroadcast(b *testing.B) {
	d := NewDisruptor[int](disruptorBenchSize)
	readers := make([]*Reader[int], 4)
	for i := range readers {
		readers[i] = d.NewReader()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Publish(i)
		for _, r := range readers {
			r.Next()
		}
	}
}

func BenchmarkCompare_MultiRingBuffer(b *testing.B) {
	// Simulating broadcast with 4 separate ring buffers
	buffers := make([]*RingBuffer[int], 4)
	for i := range buffers {
		buffers[i] = NewRingBuffer[int](disruptorBenchSize)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Must publish to all buffers (duplicate writes)
		for _, rb := range buffers {
			rb.Put(i)
		}
		// Each consumer reads from its buffer
		for _, rb := range buffers {
			rb.Poll(0)
		}
	}
}
