// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bytes"
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
)

func TestCodec(t *testing.T) {
	const N = 100
	fz := fuzz.New()
	fz.NilChance(0)
	fz.NumElements(N, N)
	var (
		c0 []string
		c1 []testStruct
	)
	fz.Fuzz(&c0)
	fz.Fuzz(&c1)

	var b bytes.Buffer
	enc := NewEncoder(&b)
	in := []reflect.Value{reflect.ValueOf(c0), reflect.ValueOf(c1)}
	if err := enc.Encode(in...); err != nil {
		t.Fatal(err)
	}
	out := []reflect.Value{
		reflect.New(reflect.SliceOf(typeOfString)),
		reflect.New(reflect.SliceOf(typeOfTestStruct)),
	}
	dec := NewDecoder(&b)
	if err := dec.Decode(out...); err != nil {
		t.Fatal(err)
	}
	for i := range in {
		if !reflect.DeepEqual(in[i].Interface(), out[i].Elem().Interface()) {
			t.Errorf("column %d mismatch", i)
		}
	}
	// Make sure we don't reallocate if we're providing slices with enough
	// capacity already.
	outptrs := make([]uintptr, len(out))
	for i := range out {
		outptrs[i] = out[i].Pointer() // points to the slice header's data
	}
	if err := enc.Encode(in...); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(out...); err != nil {
		t.Fatal(err)
	}
	for i := range out {
		if outptrs[i] != out[i].Pointer() {
			t.Errorf("column slice %d reallocated", i)
		}
	}
}
