// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"bytes"
	"context"
	"math/rand"
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicetype"
)

type testStruct struct{ A, B, C int }

var (
	typeOfString     = reflect.TypeOf("")
	typeOfTestStruct = reflect.TypeOf((*testStruct)(nil)).Elem()
	typeOfInt        = reflect.TypeOf(0)
	typeOfSliceOfInt = reflect.SliceOf(typeOfInt)
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

	in := frame.Slices(c0, c1)
	if err := enc.Encode(in); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(in); err != nil {
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
	for i := 0; i < in.NumOut(); i++ {
		if !reflect.DeepEqual(in.Interface(i), out[i].Elem().Interface()) {
			t.Errorf("column %d mismatch", i)
		}
	}
	// Make sure we don't reallocate if we're providing slices with enough
	// capacity already.
	outptrs := make([]uintptr, len(out))
	for i := range out {
		outptrs[i] = out[i].Pointer() // points to the slice header's data
	}
	if err := enc.Encode(in); err != nil {
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

func TestDecodingReader(t *testing.T) {
	const N = 10000
	fz := fuzz.New()
	fz.NilChance(0)
	fz.NumElements(N, N)
	var (
		col1 []string
		col2 []int
		col3 [][]int
	)
	fz.Fuzz(&col1)
	fz.Fuzz(&col2)
	fz.Fuzz(&col3)
	var buf Buffer
	for i := 0; i < len(col1); {
		// Pick random batch size.
		n := int(rand.Int31n(int32(len(col1) - i + 1)))
		c1, c2, c3 := col1[i:i+n], col2[i:i+n], col3[i:i+n]
		if err := buf.WriteColumns(reflect.ValueOf(c1), reflect.ValueOf(c2), reflect.ValueOf(c3)); err != nil {
			t.Fatal(err)
		}
		i += n
	}

	r := NewDecodingReader(&buf)
	var (
		col1x []string
		col2x []int
		col3x [][]int
	)
	if err := ReadAll(context.Background(), r, &col1x, &col2x, &col3x); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(col1, col1x) {
		t.Error("col1 mismatch")
	}
	if !reflect.DeepEqual(col2, col2x) {
		t.Error("col2 mismatch")
	}
	if !reflect.DeepEqual(col3, col3x) {
		t.Error("col3 mismatch")
	}
}

func TestDecodingReaderWithZeros(t *testing.T) {
	// Gob, in its infinite cleverness, does not transmit zero values.
	// However, it apparently also does not zero out zero values in
	// structs that are reused. This requires special handling that is
	// tested here.
	type fields struct{ A, B, C int }
	var b bytes.Buffer
	in := []fields{{1, 2, 3}, {1, 0, 3}}
	enc := NewEncoder(&b)
	enc.Encode(frame.Slices(in[0:1]))
	enc.Encode(frame.Slices(in[1:2]))

	r := NewDecodingReader(&b)
	ctx := context.Background()

	var out []fields
	if err := ReadAll(ctx, r, &out); err != nil {
		t.Fatal(err)
	}

	if got, want := out, in; !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestDecodingSlices(t *testing.T) {
	// Gob will reuse slices during decoding if we're not careful.
	var b bytes.Buffer
	in := [][]string{{"a", "b"}, {"c", "d"}}
	enc := NewEncoder(&b)
	enc.Encode(frame.Slices(in[0:1]))
	enc.Encode(frame.Slices(in[1:2]))

	r := NewDecodingReader(&b)
	ctx := context.Background()
	var out [][]string
	if err := ReadAll(ctx, r, &out); err != nil {
		t.Fatal(err)
	}
	if got, want := out, in; !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestEmptyDecodingReader(t *testing.T) {
	r := NewDecodingReader(bytes.NewReader(nil))
	f := frame.Make(slicetype.New(typeOfString, typeOfInt), 100, 100)
	n, err := r.Read(context.Background(), f)
	if got, want := n, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := err, EOF; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	n, err = r.Read(context.Background(), f)
	if got, want := n, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := err, EOF; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func testRoundTrip(t *testing.T, cols ...interface{}) {
	t.Helper()
	var N = 1000
	if testing.Short() {
		N = 10
	}
	var Stride = N / 5
	fz := fuzz.New()
	fz.NilChance(0)
	fz.NumElements(N, N)
	for i := range cols {
		ptr := reflect.New(reflect.TypeOf(cols[i]))
		fz.Fuzz(ptr.Interface())
		cols[i] = reflect.Indirect(ptr).Interface()
	}
	var b bytes.Buffer
	enc := NewEncoder(&b)
	for i := 0; i < N; i += Stride {
		j := i + Stride
		if j > N {
			j = N
		}
		args := make([]interface{}, len(cols))
		for k := range args {
			args[k] = reflect.ValueOf(cols[k]).Slice(i, j).Interface()
		}
		if err := enc.Encode(frame.Slices(args...)); err != nil {
			t.Fatal(err)
		}
	}
	args := make([]interface{}, len(cols))
	for i := range args {
		// Create an empty slice from the end of the parent slice.
		slice := reflect.ValueOf(cols[i]).Slice(N, N)
		ptr := reflect.New(slice.Type())
		reflect.Indirect(ptr).Set(slice)
		args[i] = ptr.Interface()
	}
	if err := ReadAll(context.Background(), NewDecodingReader(&b), args...); err != nil {
		t.Fatal(err)
	}
	for i, want := range cols {
		got := reflect.Indirect(reflect.ValueOf(args[i])).Interface()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestTypes(t *testing.T) {
	types := [][]interface{}{
		{[]int{}, []string{}},
		{[]struct{ A, B, C int }{}},
		{[][]string{}, []int{}},
		{[]struct {
			A *int
			B string
		}{}, []*int{}},
		{[]rune{}, []byte{}, [][]byte{}, []int16{}, []int8{}, []*[]string{}, []int64{}},
	}
	for _, cols := range types {
		testRoundTrip(t, cols...)
	}
}
