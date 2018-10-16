// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package zero_test

import (
	"fmt"
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/internal/zero"
)

var zeroTypes = []reflect.Type{
	reflect.TypeOf(int8(0)),
	reflect.TypeOf(int16(0)),
	reflect.TypeOf(int32(0)),
	reflect.TypeOf(int64(0)),
	reflect.TypeOf(0),
	reflect.TypeOf(""),
	reflect.TypeOf([]byte{}),
	reflect.TypeOf(struct{ A, B int }{}),
	reflect.TypeOf([]int{}),
	reflect.TypeOf([10]int16{}),
	reflect.TypeOf([]*int{}),
	reflect.TypeOf(struct{ A, B *int }{}),
}

func TestZeroSlice(t *testing.T) {
	const N = 100
	fz := fuzz.New()

	for _, typ := range zeroTypes {
		slice := reflect.MakeSlice(reflect.SliceOf(typ), N, N)
		for i := 0; i < N; i++ {
			fz.Fuzz(slice.Index(i).Addr().Interface())
		}
		// We have to pass a slice in
		zero.Slice(slice)
		zeroValue := reflect.Zero(typ)
		for i := 0; i < N; i++ {
			if got, want := slice.Index(i), zeroValue; !reflect.DeepEqual(got.Interface(), want.Interface()) {
				t.Errorf("got %v, want %v", got, want)
			}
		}
	}
}

func BenchmarkZero(b *testing.B) {
	const N = 1 << 20
	for _, typ := range zeroTypes {
		b.Run(fmt.Sprintf("type=%v", typ), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(N)
			n := int(N / typ.Size())
			slice := reflect.MakeSlice(reflect.SliceOf(typ), n, n)
			zero.Slice(slice)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				zero.Slice(slice)
			}
		})
	}
}
