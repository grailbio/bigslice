// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice_test

import (
	"fmt"
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/slicetest"
)

func TestReduce(t *testing.T) {
	const N = 100
	ints := make([]int, N)
	for i := range ints {
		ints[i] = i
	}
	for m := 1; m < 5; m++ {
		slice := bigslice.Const(m, ints)
		slice = bigslice.Map(slice, func(x int) (string, int) {
			return fmt.Sprint(x%3) + "x", x
		})
		slice = bigslice.Reduce(slice, func(x, y int) int { return x + y })
		assertEqual(t, slice, true, []string{"0x", "1x", "2x"}, []int{1683, 1617, 1650})
	}
}

func TestReducePrefix(t *testing.T) {
	const N = 100
	ints := make([]int, N)
	for i := range ints {
		ints[i] = i
	}
	for m := 1; m < 5; m++ {
		slice := bigslice.Const(m, ints)
		slice = bigslice.Map(slice, func(x int) (string, int, int) {
			return "x", x % 3, x
		})
		slice = bigslice.Prefixed(slice, 2)
		slice = bigslice.Reduce(slice, func(x, y int) int { return x + y })
		assertEqual(t, slice, true, []string{"x", "x", "x"}, []int{0, 1, 2}, []int{1683, 1617, 1650})
	}
}

func ExampleReduce() {
	slice := bigslice.Const(2,
		[]string{"c", "a", "b", "c", "c", "b", "a", "a", "a", "a", "c"},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
	)
	slice = bigslice.Reduce(slice, func(a, b int) int { return a + b })
	slicetest.Print(slice)
	// Output:
	// a 5
	// b 2
	// c 4
}
