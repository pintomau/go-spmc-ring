package ringring

import "context"

// initPool is a shared helper that allocates and wires a BitmapReaderPool[T]
// against the ring buffer's backing slice. upstream=nil defaults to the writer cursor.
func (r *RingBuffer[T]) initPool(upstream WriterBarrier) *BitmapReaderPool[T] {
	if upstream == nil {
		upstream = &r.writeCursor
	}
	pool := &BitmapReaderPool[T]{}
	pool.buffer = r.buffer
	pool.mask = r.mask
	pool.writerCursor = upstream
	pool.rootCtx, pool.rootCancel = context.WithCancel(r.barrier.rootCtx)
	writerPos := r.writeCursor.Load()
	for i := range 128 {
		pool.slots[i].cursor.Store(writerPos)
	}
	return pool
}

// Stage is a named pipeline stage. Each stage owns a BitmapReaderPool and exposes:
//   - Barrier() — a concurrent-safe WriterBarrier for wiring a downstream stage
//   - Load()    — the cached WriterBarrier the writer uses for gating (single-threaded)
type Stage[T any] struct {
	pool *BitmapReaderPool[T]
}

func (s *Stage[T]) AddReader(fn ReaderFunc[T]) (int, error) { return s.pool.AddReader(fn) }
func (s *Stage[T]) RemoveReader(slotId int) error           { return s.pool.RemoveReader(slotId) }
func (s *Stage[T]) Shutdown()                               { s.pool.Shutdown() }

// Barrier returns a concurrent-safe, stateless WriterBarrier over this stage's
// minimum cursor. Pass this to rb.NewStage() as the upstream argument when wiring
// a downstream stage. Never pass to SetGatingStage — that path requires the cached
// Load() for performance.
func (s *Stage[T]) Barrier() WriterBarrier { return &poolBarrier[T]{pool: s.pool} }

// Load implements WriterBarrier using the cached scan path. Called only by the
// single writer goroutine via SetGatingStage.
func (s *Stage[T]) Load() int64 { return s.pool.Load() }

// NewStage creates a pipeline stage gated by upstream. Pass nil to gate on the
// writer cursor. Wire a two-stage pipeline:
//
//	s1 := rb.NewStage(nil)
//	s2 := rb.NewStage(s1.Barrier())
//	rb.SetGatingStage(s2)
func (r *RingBuffer[T]) NewStage(upstream WriterBarrier) *Stage[T] {
	return &Stage[T]{pool: r.initPool(upstream)}
}

// SetGatingStage points the writer's backpressure at the leaf stage. Only accepts
// *Stage[T] (not a bare WriterBarrier) to guarantee the cached Load() path is used.
func (r *RingBuffer[T]) SetGatingStage(leaf *Stage[T]) {
	r.gatingBarrier = leaf
}
