package cdv_disruptor

import (
	"context"
	"maps"
	"math/rand/v2"
	"sync/atomic"
	"testing"
)

func BenchmarkBitmapReaderPool_Load(b *testing.B) {
	ctx := context.Background()
	writerCursor := &atomic.Int64{}

	// Helper to setup pool with N readers
	setupPool := func(n int) *BitmapReaderPool[int] {
		pool := NewBitmapReaderPool[int](ctx, writerCursor)
		for i := 0; i < n; i++ {
			pool.AddReader(func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
				<-ctx.Done()
			})
		}
		return pool
	}

	b.Run("BestCase_Gating_128Readers", func(b *testing.B) {
		pool := setupPool(128)
		pool.Load() // Prime the cache

		for b.Loop() {
			pool.Load()
		}
	})

	b.Run("ScanAll_1Readers", func(b *testing.B) {
		pool := setupPool(1)
		// Pre-load bitmaps to avoid atomic load overhead in the loop if we want to test just the scan
		// But scanAll takes arguments, so we can pass them.
		bm0 := pool.activeSlots[0].Load()
		bm1 := pool.activeSlots[1].Load()

		for b.Loop() {
			pool.scanAll(bm0, bm1)
		}
	})

	b.Run("ScanAll_64Readers", func(b *testing.B) {
		pool := setupPool(64)
		// Pre-load bitmaps to avoid atomic load overhead in the loop if we want to test just the scan
		// But scanAll takes arguments, so we can pass them.
		bm0 := pool.activeSlots[0].Load()
		bm1 := pool.activeSlots[1].Load()

		for b.Loop() {
			pool.scanAll(bm0, bm1)
		}
	})

	b.Run("ScanAll_128Readers", func(b *testing.B) {
		pool := setupPool(128)
		bm0 := pool.activeSlots[0].Load()
		bm1 := pool.activeSlots[1].Load()

		for b.Loop() {
			pool.scanAll(bm0, bm1)
		}
	})

	b.Run("Dynamic_Churn_64Readers", func(b *testing.B) {
		pool := NewBitmapReaderPool[int](ctx, writerCursor)
		// Fill half
		for i := 0; i < 64; i++ {
			pool.AddReader(func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
				<-ctx.Done()
			})
		}

		// Start background churn
		churnCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			for {
				select {
				case <-churnCtx.Done():
					return
				default:
					// Add one
					id, err := pool.AddReader(func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
						<-ctx.Done()
					})
					if err == nil {
						pool.RemoveReader(id)
					}
				}
			}
		}()

		for b.Loop() {
			pool.Load()
		}
	})

	b.Run("Dynamic_Churn_64Readers_Random", func(b *testing.B) {
		readers := make(map[int]struct{})
		pool := NewBitmapReaderPool[int](ctx, writerCursor)
		// Fill half
		for i := 0; i < 64; i++ {
			id, _ := pool.AddReader(func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
				<-ctx.Done()
			})
			readers[id] = struct{}{}
		}

		// Start background churn
		churnCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			for {
				select {
				case <-churnCtx.Done():
					return
				default:

					shouldAdd := rand.IntN(2)
					if shouldAdd == 1 {
						// Add one
						id, err := pool.AddReader(func(ctx context.Context, readView ReadView[int], readerCursor *atomic.Int64) {
							<-ctx.Done()
						})
						if err == nil {
							pool.RemoveReader(id)
							continue
						}
						readers[id] = struct{}{}
					} else {
						lenReaders := len(readers)
						if lenReaders == 0 {
							continue
						}

						n := rand.IntN(lenReaders)
						var toRemove int
						for toRemove = range maps.Keys(readers) {
							if n == 0 {
								break
							}
							n--
						}

						pool.RemoveReader(toRemove)
						delete(readers, toRemove)
					}
				}
			}
		}()

		for b.Loop() {
			pool.Load()
		}
	})
}
