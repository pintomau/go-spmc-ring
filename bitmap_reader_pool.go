package ringring

import (
	"context"
	"errors"
	"iter"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxInt64 = int64(math.MaxInt64)

	// Slot Control State
	slotStateAllocated = 1 << 0 // 1
	slotStateActive    = 1 << 1 // 2
	slotStateRunning   = 1 << 2 // 4
)

// BitmapReaderPool supports up to 128 dynamic readers with zero false sharing
type BitmapReaderPool[T any] struct {
	//_ [CacheLineSize]byte
	// # Cache line 0: Writer's Hot Path Metadata
	// Two 64-bit words cover 128 readers (bits 0-63 and 64-127)
	activeSlots [2]atomic.Uint64 // 16 bytes - reader activity tracking
	// Writer cache
	lastMinimum int64                                    // 8 bytes - cached minimum value
	lastBitmap0 uint64                                   // 8 bytes - cached bitmap word 0
	lastBitmap1 uint64                                   // 8 bytes - cached bitmap word 1
	gatingSlot  int                                      // 8 bytes - cached slowest reader index
	_           [CacheLineSize - 16 - 8 - 8 - 8 - 8]byte // prevent false sharing

	// # Cache line 1: Allocation Metadata (cold path. add/remove operations)
	buffer         []T                                    // 24 bytes
	allocatedSlots [2]atomic.Uint64                       // 16 bytes - slot allocation bitmap
	writerCursor   WriterBarrier                          // 16 bytes - upstream barrier (*atomic.Int64 or upstream pool)
	mask           int64                                  // 8 bytes
	_              [CacheLineSize - 24 - 16 - 16 - 8]byte // prevent false sharing

	// # Cache lines 2-129: Reader Cursors (each gets own cache line)
	// Each cursor padded to CacheLineSize to prevent false sharing between readers
	slots [128]struct {
		cursor atomic.Int64 // 8 bytes - the actual cursor position
		_      [CacheLineSize - 8]byte
	}

	// # Cache Lines 130-257: Slot Control State
	// Each slot's control data in separate cache line to avoid interfering with hot path
	slotControl [128]struct {
		readerFunc atomic.Value    // 16 bytes
		ctx        context.Context // 16 bytes - reader's context
		cancelFn   atomic.Value    // 16 bytes - cancellation func (atomic to prevent races)

		state atomic.Uint32 // 4 bytes

		_ [CacheLineSize - 16 - 16 - 16 - 4]byte // Pad to CacheLineSize
	}

	// # Lifecycle Management
	rootCtx    context.Context    // Root context for all slots
	rootCancel context.CancelFunc // Global shutdown trigger
	wg         sync.WaitGroup     // Wait for internal goroutines
}

func NewBitmapReaderPool[T any](ctx context.Context, writerCursor WriterBarrier) *BitmapReaderPool[T] {
	b := &BitmapReaderPool[T]{}

	// initialize all cursors to writer position
	writerPos := writerCursor.Load()
	for i := range 128 {
		b.slots[i].cursor.Store(writerPos)
	}

	b.rootCtx, b.rootCancel = context.WithCancel(ctx)
	b.writerCursor = writerCursor

	return b
}

type ReaderFunc[T any] func(ctx context.Context, readView ReadView[T], readerCursor *atomic.Int64)

func (b *BitmapReaderPool[T]) AddReader(fn ReaderFunc[T]) (int, error) {
	// 1. Lock-free slot allocation using CAS loop (cache line 1)
	slotId := b.allocateSlot()
	if slotId == -1 {
		return -1, errors.New("no free slots available")
	}

	// Initialize cursor to current writer position
	b.slots[slotId].cursor.Store(b.writerCursor.Load())
	control := &b.slotControl[slotId]
	control.readerFunc.Store(fn)

	// Atomically set the state to Allocated + Active + Running
	oldState := control.state.Swap(slotStateAllocated | slotStateActive | slotStateRunning)

	b.activateSlot(slotId)

	// If the "running" bit was NOT set in the old state, it means the goroutine
	// had terminated (or never started). We must spawn a new one
	if oldState&slotStateRunning == 0 {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.runSlot(slotId)
		}()
	}

	return slotId, nil
}

func (b *BitmapReaderPool[T]) runSlot(slotId int) {
	const initialBackoff = 10 * time.Millisecond
	const maxBackoff = 100 * time.Millisecond
	const backoffMultiplier = 2.0
	const terminateAfter = 1 * time.Minute
	const terminateThreshold = int(terminateAfter / initialBackoff)

	control := &b.slotControl[slotId]
	currentBackoff := initialBackoff
	idleLoops := 0

	for {
		select {
		case <-b.rootCtx.Done():
			return
		default:
		}

		currentState := control.state.Load()

		// idle
		if currentState&slotStateActive == 0 {
			idleLoops++

			// check if termination threshold
			if idleLoops >= terminateThreshold {
				// Attempt to transition: Running -> Stopped
				// We expect the state to be just 'Running'.
				// if AddReader intervenes, state will change to (Allocated|Active|Running), causing CAS to fail.
				if control.state.CompareAndSwap(currentState, 0) {
					// We have officially stopped. goroutine off.
					return
				}

				// CAS failed: Work arrived just in time. Reset idle stats.
				idleLoops = 0
				currentBackoff = initialBackoff
				continue
			}

			// exponential backoff
			select {
			case <-b.rootCtx.Done():
				return
			case <-time.After(currentBackoff):
			}

			// increate backoff for next iteration
			currentBackoff = time.Duration(float64(currentBackoff) * backoffMultiplier)
			if currentBackoff > maxBackoff {
				currentBackoff = maxBackoff
			}

			continue
		}

		// slot activated. reset backoff state
		idleLoops = 0
		currentBackoff = initialBackoff

		// cancelable context for this activation
		ctx, cancel := context.WithCancel(b.rootCtx)
		control.ctx = ctx
		control.cancelFn.Store(cancel)

		// barrier check: Did RemoveReader run while we were setting up?
		if control.state.Load()&slotStateActive == 0 {
			cancel()
		} else {
			readView := ReadView[T]{
				buffer:       b.buffer,
				writerCursor: b.writerCursor,
				mask:         b.mask,
			}

			readerFunc := control.readerFunc.Load().(ReaderFunc[T])
			// run the reader func (and pass pointer to atomic cursor)
			// blocks until reader is finished
			readerFunc(control.ctx, readView, &b.slots[slotId].cursor)
		}

		// Cleanup
		// Transition to idle (Running only).
		// This clears Active and Allocated bits atomically.
		// We keep 'Running' so the goroutine stays alive for reuse.
		control.state.Store(slotStateRunning)

		// Deactivate from writer's view on Load
		b.deactivateSlot(slotId)
		var nilFunc ReaderFunc[T]
		control.readerFunc.Store(nilFunc)
		control.ctx = nil
		var nilCancel context.CancelFunc
		control.cancelFn.Store(nilCancel)
		// Make slot available to AddReader
		b.deallocateSlot(slotId)
	}
}

func (b *BitmapReaderPool[T]) RemoveReader(slotId int) error {
	if slotId < 0 || slotId >= 128 {
		return errors.New("invalid slot ID")
	}

	control := &b.slotControl[slotId]

	// 1. Atomically clear the active bit FIRST
	// This ensures the runSlot loop will see it's time to idle/stop
	// and serves as a barrier for the cancelFn check.
	for {
		oldState := control.state.Load()
		if oldState&slotStateActive == 0 {
			break // Already inactive
		}

		newState := oldState &^ slotStateActive
		if control.state.CompareAndSwap(oldState, newState) {
			break
		}
	}

	// 2. Cancel context to unblock the user function
	// We do this AFTER clearing Active bit.
	// If runSlot is setting up, it will see Active=0 and cancel itself.
	// If runSlot is already running, we cancel it here.
	val := control.cancelFn.Load()
	if fn, ok := val.(context.CancelFunc); ok && fn != nil {
		fn()
	}

	// Deactivate from writer's view on Load immediately
	b.deactivateSlot(slotId)

	return nil
}

// allocateSlot allocates a slot using a lock-free CAS loop
func (b *BitmapReaderPool[T]) allocateSlot() int {
	const fullBitmap = ^uint64(0)
	for {
		// try word 0 (slots 0..63)
		bitmap0 := b.allocatedSlots[0].Load()
		if bitmap0 != fullBitmap { // has at least one free bit
			slotId := bits.TrailingZeros64(^bitmap0)
			mask := uint64(1) << slotId

			// try to claim this slot with CAS
			if b.allocatedSlots[0].CompareAndSwap(bitmap0, bitmap0|mask) {
				return slotId
			}

			// CAS failed, retry (another goroutine claimed it)
			continue
		}

		// try word 1 (slots 64..127)
		bitmap1 := b.allocatedSlots[1].Load()
		if bitmap1 != fullBitmap {
			slotId := bits.TrailingZeros64(^bitmap1)
			mask := uint64(1) << slotId

			if b.allocatedSlots[1].CompareAndSwap(bitmap1, bitmap1|mask) {
				return slotId + 64
			}
			continue
		}

		// no free slots
		return -1
	}
}

func (b *BitmapReaderPool[T]) deallocateSlot(slotId int) {
	wordIdx := slotId >> 6
	bitIdx := slotId & 63
	mask := ^(uint64(1) << bitIdx)

	// deactivate the bit within the bitmap
	b.allocatedSlots[wordIdx].And(mask)
}

func (b *BitmapReaderPool[T]) activateSlot(slotId int) {
	wordIdx := slotId >> 6 // divide by 64 to get which word to use (1 or 2)
	// remainder of division by 64 (equivalent to slotId % 64)
	// example:
	// slotId = 100
	//		Binary: 0b01100100 & 0b00111111 = 0b00100100 = 36
	//
	// slotId = 127
	//		Binary: 0b01111111 & 0b00111111 = 0b00111111 = 63
	bitIdx := slotId & 63
	mask := uint64(1) << bitIdx

	// activate the bit within the bitmap
	b.activeSlots[wordIdx].Or(mask)
}

func (b *BitmapReaderPool[T]) deactivateSlot(slotId int) {
	wordIdx := slotId >> 6
	bitIdx := slotId & 63
	mask := ^(uint64(1) << bitIdx)

	// deactivate the bit within the bitmap
	b.activeSlots[wordIdx].And(mask)
}

// Load returns minimum cursor across all active readers
//
// Called by writer in reserve() loop (Hot Path)
//
//   - Optimized with "Gating" strategy to avoid scanning all readers on every call.
//   - Fast path: Only cache line 0 (bitmap + cache in same line)
//   - Slow path: cache line 0 + cursor cache lines (2-129)
//   - Never touches control cache lines (130-257)
//   - Zero false sharing with reader updates
func (b *BitmapReaderPool[T]) Load() int64 {
	bitmap0 := b.activeSlots[0].Load()
	bitmap1 := b.activeSlots[1].Load()

	// topology check
	// if readers were added/removed, our gating slot might be invalid. Force full scan.
	if bitmap0 != b.lastBitmap0 || bitmap1 != b.lastBitmap1 {
		return b.scanAll(bitmap0, bitmap1)
	}

	// fast path: gating check
	// if there are no readers, return writer position
	if bitmap0 == 0 && bitmap1 == 0 {
		return b.writerCursor.Load()
	}

	// we access the array directly using the cached index.
	gatingVal := b.slots[b.gatingSlot].cursor.Load()

	// if the slowest reader hasn't moved, the global minimum definitely hasn't changed.
	// we can return immediately, saving memory loads
	if gatingVal == b.lastMinimum {
		return b.lastMinimum
	}

	// slow path: full scan
	// the slowest reader moved! it might still be the slowest, or someone else might be.
	// we must scan everyone to find the new minimum
	return b.scanAll(bitmap0, bitmap1)
}

// scanAll iterates all active slots to find the minimum and updates the cache.
func (b *BitmapReaderPool[T]) scanAll(bitmap0 uint64, bitmap1 uint64) int64 {
	// We use two independent accumulators (A and B) to break data dependency chains.
	// Enables Instruction Level Parallelism using Loop Unrolling for faster min-finding
	maxPosition := b.writerCursor.Load()
	minA, minB := maxPosition, maxPosition
	gatingA, gatingB := 0, 0

	// Scan Word 0 (Slots 0-63)
	tmpMap := bitmap0
	for tmpMap != 0 {
		// Path A
		slotIdA := bits.TrailingZeros64(tmpMap)
		tmpMap &= tmpMap - 1
		// slotIdA is 0..63, so it is within bounds.
		valA := b.slots[slotIdA].cursor.Load()
		if valA < minA {
			minA = valA
			gatingA = slotIdA
		}

		// Check if we are done before starting Path B
		if tmpMap == 0 {
			break
		}

		// Path B
		slotIdB := bits.TrailingZeros64(tmpMap)
		tmpMap &= tmpMap - 1
		valB := b.slots[slotIdB].cursor.Load()
		if valB < minB {
			minB = valB
			gatingB = slotIdB
		}
	}

	// Scan Word 1 (Slots 64-127)
	tmpMap = bitmap1
	for tmpMap != 0 {
		// Path A
		slotIdA := bits.TrailingZeros64(tmpMap) + 64
		tmpMap &= tmpMap - 1
		valA := b.slots[slotIdA].cursor.Load()
		if valA < minA {
			minA = valA
			gatingA = slotIdA
		}

		if tmpMap == 0 {
			break
		}

		// Path B
		slotIdB := bits.TrailingZeros64(tmpMap) + 64
		tmpMap &= tmpMap - 1
		valB := b.slots[slotIdB].cursor.Load()
		if valB < minB {
			minB = valB
			gatingB = slotIdB
		}
	}

	// Calculate final minimum
	if minB < minA {
		minA = minB
		gatingA = gatingB
	}

	b.lastMinimum = minA
	b.gatingSlot = gatingA
	b.lastBitmap0 = bitmap0
	b.lastBitmap1 = bitmap1

	return minA
}

// scanAllStateless finds the minimum cursor across active slots without touching any
// cached fields. Safe to call concurrently from multiple goroutines (e.g. downstream
// pipeline stages whose readers all call LoadWriterBarrier simultaneously).
func (b *BitmapReaderPool[T]) scanAllStateless(bitmap0, bitmap1 uint64) int64 {
	maxPosition := b.writerCursor.Load()
	minA, minB := maxPosition, maxPosition

	tmpMap := bitmap0
	for tmpMap != 0 {
		slotIdA := bits.TrailingZeros64(tmpMap)
		tmpMap &= tmpMap - 1
		if v := b.slots[slotIdA].cursor.Load(); v < minA {
			minA = v
		}
		if tmpMap == 0 {
			break
		}
		slotIdB := bits.TrailingZeros64(tmpMap)
		tmpMap &= tmpMap - 1
		if v := b.slots[slotIdB].cursor.Load(); v < minB {
			minB = v
		}
	}

	tmpMap = bitmap1
	for tmpMap != 0 {
		slotIdA := bits.TrailingZeros64(tmpMap) + 64
		tmpMap &= tmpMap - 1
		if v := b.slots[slotIdA].cursor.Load(); v < minA {
			minA = v
		}
		if tmpMap == 0 {
			break
		}
		slotIdB := bits.TrailingZeros64(tmpMap) + 64
		tmpMap &= tmpMap - 1
		if v := b.slots[slotIdB].cursor.Load(); v < minB {
			minB = v
		}
	}

	if minB < minA {
		return minB
	}
	return minA
}

// poolBarrier is a thread-safe, stateless view of a BitmapReaderPool's minimum cursor.
// Unlike Load(), it writes no cached fields and is safe for concurrent callers
// (e.g. multiple readers in a downstream pipeline stage).
type poolBarrier[T any] struct{ pool *BitmapReaderPool[T] }

func (p *poolBarrier[T]) Load() int64 {
	b0 := p.pool.activeSlots[0].Load()
	b1 := p.pool.activeSlots[1].Load()
	if b0 == 0 && b1 == 0 {
		// empty stage: propagate upstream position unchanged
		return p.pool.writerCursor.Load()
	}
	return p.pool.scanAllStateless(b0, b1)
}

// Shutdown stops all internal goroutines and waits for them to exit.
func (b *BitmapReaderPool[T]) Shutdown() {
	b.rootCancel()
	b.wg.Wait()
}

type ReadView[T any] struct {
	buffer       []T           // 24 bytes
	writerCursor WriterBarrier // 16 bytes
	mask         int64         // 8 bytes
}

func (r *ReadView[T]) Get(seq int64) *T {
	return &r.buffer[seq&r.mask]
}

func (r *ReadView[T]) GetRange(start, end int64) []T {
	if start <= end {
		// contiguous segment
		return r.buffer[start : end+1]
	}

	result := make([]T, 0, (len(r.buffer)-int(start))+(int(end)+1))
	result = append(result, r.buffer[start:]...)
	result = append(result, r.buffer[:end+1]...)
	return result
}

func (r *ReadView[T]) Iterate(start, end int64) iter.Seq[*T] {
	return func(yield func(*T) bool) {
		if start == end {
			yield(r.Get(start))
			return
		}

		for maskedStart := start & r.mask; maskedStart != end; maskedStart = (maskedStart + 1) & r.mask {
			if !yield(&r.buffer[maskedStart]) {
				break
			}
		}
	}
}

func (r *ReadView[T]) GetMask() int64 {
	return r.mask
}

func (r *ReadView[T]) LoadWriterBarrier() int64 {
	return r.writerCursor.Load()
}
