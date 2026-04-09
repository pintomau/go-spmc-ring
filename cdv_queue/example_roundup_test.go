package cdv_queue

import (
	"fmt"
)

func RoundUp(v uint64) uint64 {
	return roundUp(v)
}

func ExampleRoundUp_nine() {
	up := roundUp(9)
	/*
		v = 9
		Binary: 0000...00001001₂
		v--
		9 - 1 = 8 → 0000...00001000₂
		Propagate bits:
		v |= v >> 1
		00001000 | 00000100 = 00001100
		v |= v >> 2
		00001100 | 00000011 = 00001111
		v |= v >> 4, ... no further changes; stays 00001111.
		Now v = 15 (00001111₂).
		v++
		15 + 1 = 16 → 00010000₂
	*/
	fmt.Printf("%d", up)
	// Output: 16
}

func ExampleRoundUp_eight() {
	up := roundUp(8)
	/*
		v = 8
		Binary: 000...00001000₂
		v--
		8 - 1 = 7 -> 0000...00000111₂
		Propagate bits:
		Already 00000111; Shits keeps it 00000111
		v++
		7 + 1 = 8
	*/
	fmt.Printf("%d", up)
	// Output: 8
}
