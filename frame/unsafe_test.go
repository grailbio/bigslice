// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package frame

import (
	"reflect"
	"testing"
)

/*

func TestUnsafeAssign(t *testing.T) {
	var src, dst int

	src = 123
	srcp, dstp := unsafe.Pointer(&src), unsafe.Pointer(&dst)
	assign(type2rtype(typeOfInt), dstp, srcp)
	if got, want := dst, src; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestUnsafeAssignBig(t *testing.T) {
	var src, dst struct {
		a, b, c int
		d       [128]byte
	}
	fz := fuzz.New()
	fz.Fuzz(&src.a)
	fz.Fuzz(&src.b)
	fz.Fuzz(&src.c)
	fz.Fuzz(&src.d)
	if got, want := src, dst; got == want {
		t.Fatal("fuzzing broken")
	}

	assign(type2rtype(reflect.TypeOf(dst)), unsafe.Pointer(&dst), unsafe.Pointer(&src))
	if got, want := src, dst; got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
*/

func TestPointers(t *testing.T) {
	types := []struct {
		val      interface{}
		pointers bool
	}{
		{struct{ x, y int }{}, false},
		{0, false},
		{new(int), true},
		{struct {
			x int
			y string
		}{}, true},
		{[]int{}, true},
		{123, false},
		{"okay", true},
		{[123]int{}, false},
		{[123]string{}, true},
		{map[int]int{}, true},
		{make(chan int), true},
	}
	for i, typ := range types {
		if got, want := pointers(reflect.TypeOf(typ.val)), typ.pointers; got != want {
			t.Errorf("test %d: %v: got %v, want %v", i, typ.val, got, want)
		}
	}
}
