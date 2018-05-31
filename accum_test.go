// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/sliceio"
)

var accumulableTypes = []reflect.Type{typeOfString, typeOfInt, typeOfInt64}

func TestAccumulator(t *testing.T) {
	fz := fuzz.New()
outer:
	for _, typ := range accumulableTypes {
		if !canMakeAccumulatorForKey(typ) {
			t.Errorf("expected to be able to make accumulator for %s", typ)
			continue
		}
		step := reflect.ValueOf(func(a, e int) int { return a + e })
		accum := makeAccumulator(typ, typeOfInt, step)
		const N = 100
		for i := 0; i < N; i++ {
			keysPtr := reflect.New(reflect.SliceOf(typ))
			fz.Fuzz(keysPtr.Interface())
			keys := keysPtr.Elem()
			counts := make([]int, keys.Len())
			for i := range counts {
				counts[i] = 1
			}
			accum.Accumulate([]reflect.Value{keys, reflect.ValueOf(counts)}, keys.Len())
			accum.Accumulate([]reflect.Value{keys, reflect.ValueOf(counts)}, keys.Len())
		}
		keys := reflect.MakeSlice(reflect.SliceOf(typ), N, N)
		vals := reflect.MakeSlice(reflect.SliceOf(typeOfInt), N, N)
		for {
			n, err := accum.Read(keys, vals)
			for i := 0; i < n; i++ {
				if vals.Index(i).Int()%2 != 0 {
					t.Errorf("odd count for key %v", keys.Index(i))
				}
			}
			if err == sliceio.EOF {
				break
			} else if err != nil {
				t.Errorf("unexpected error %v", err)
				continue outer
			}
		}
	}
}
