//go:build goexperiment.simd

// This example requires Go 1.26+ compiled with GOEXPERIMENT=simd on amd64;
// without the experiment the build tag excludes this file entirely, so the
// module keeps building for everyone else.
//
// It demonstrates the one simd pattern that measurably pays off with this
// library: filling Reserve segments with generated payloads (a template
// kept in a vector register plus a per-slot sequence stamp). Measured at
// about 3.5x over a scalar PublishBatchFunc for batches of 64 and up when
// the ring is cache resident; plain copies gain nothing over PublishBatch.
// Background and benchmark tables: see the "SIMD segment fills" section in
// README.md.
package spmc_test

import (
	"context"
	"fmt"
	"simd/archsimd"
	"unsafe"

	"github.com/pintomau/go-spmc-ring"
)

// simdEvent is exactly one cache line, so each element is a single Uint8x64
// load or store. Payload sizes that are a multiple of 64 also land on
// 64-byte-aligned bases by allocator behavior, which keeps every vector
// store within one cache line.
type simdEvent struct {
	x [64]byte
}

// fillSegment stamps every slot with the template, then overwrites the
// first 8 bytes with the slot's sequence number. The template stays in a
// vector register across the whole segment. The scalar fallback is
// byte-identical, so the function is safe to call on any amd64 CPU.
func fillSegment(seg []simdEvent, template *simdEvent, seq uint64) uint64 {
	if len(seg) == 0 {
		return seq
	}
	if archsimd.X86.AVX512() {
		tv := archsimd.LoadUint8x64Slice(template.x[:])
		bs := unsafe.Slice((*byte)(unsafe.Pointer(&seg[0])), len(seg)*64)
		for i := range seg {
			tv.StoreSlice(bs[i*64 : i*64+64])
			*(*uint64)(unsafe.Pointer(&seg[i])) = seq
			seq++
		}
		return seq
	}
	for i := range seg {
		seg[i] = *template
		*(*uint64)(unsafe.Pointer(&seg[i])) = seq
		seq++
	}
	return seq
}

func ExampleRingBuffer_Reserve() {
	rb, err := spmc.NewRingBuffer[simdEvent](context.Background(), 1<<10)
	if err != nil {
		panic(err)
	}

	var template simdEvent
	for i := range template.x {
		template.x[i] = 0xAA
	}

	// Reserve returns up to two contiguous views into the ring (two only
	// when the reservation wraps the ring end). Fill both, then Commit
	// publishes the whole batch with a single atomic store.
	const n = 6
	seg1, seg2, claim := rb.Reserve(n)
	firstSeq := uint64(claim) - n + 1
	seq := fillSegment(seg1, &template, firstSeq)
	fillSegment(seg2, &template, seq)
	rb.Commit(claim)

	stamps := make([]uint64, 0, n)
	for i := range seg1 {
		stamps = append(stamps, *(*uint64)(unsafe.Pointer(&seg1[i])))
	}
	for i := range seg2 {
		stamps = append(stamps, *(*uint64)(unsafe.Pointer(&seg2[i])))
	}
	fmt.Println(stamps)
	// Output: [1 2 3 4 5 6]
}
