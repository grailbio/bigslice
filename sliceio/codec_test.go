// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicetype"
)

type testStruct struct{ A, B, C int }

func init() {
	var key = frame.FreshKey()

	frame.RegisterOps(func(slice []testStruct) frame.Ops {
		return frame.Ops{
			Encode: func(e frame.Encoder, i, j int) error {
				p, err := json.Marshal(slice[i:j])
				if err != nil {
					return err
				}
				return e.Encode(p)
			},
			Decode: func(d frame.Decoder, i, j int) error {
				var p *[]byte
				if d.State(key, &p) {
					*p = []byte{}
				}
				if err := d.Decode(p); err != nil {
					return err
				}
				x := slice[i:j]
				if err := json.Unmarshal(*p, &x); err != nil {
					return err
				}
				if len(x) != j-i {
					return errors.New("bad json decode")
				}
				return nil
			},
		}
	})
}

var (
	typeOfString = reflect.TypeOf("")
	typeOfInt    = reflect.TypeOf(0)
)

func TestCodec(t *testing.T) {
	const N = 1000
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
	enc := NewEncodingWriter(&b)

	in := frame.Slices(c0, c1)
	if err := enc.Write(in); err != nil {
		t.Fatal(err)
	}
	if err := enc.Write(in); err != nil {
		t.Fatal(err)
	}
	data := b.Bytes()
	ctx := context.Background()
	for _, chunkSize := range []int{1, N / 3, N / 2, N, N * 2} {
		dec := NewDecodingReader(bytes.NewReader(data))
		out := frame.Make(in, N*2, N*2)
		for i := 0; i < N*2; {
			j := i + chunkSize
			if j > N*2 {
				j = N * 2
			}
			n, err := dec.Read(ctx, out.Slice(i, j))
			if err != nil {
				t.Fatal(err)
			}
			i += n
		}
		for i := 0; i < in.NumOut(); i++ {
			if !reflect.DeepEqual(in.Interface(i), out.Slice(0, N).Interface(i)) {
				t.Errorf("column %d mismatch", i)
			}
			if !reflect.DeepEqual(in.Interface(i), out.Slice(N, N*2).Interface(i)) {
				t.Errorf("column %d mismatch", i)
			}
		}
		n, err := dec.Read(ctx, out)
		if got, want := err, EOF; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := n, 0; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	/*
		// Make sure we don't reallocate if we're providing slices with enough
		// capacity already.
		outptrs := make([]uintptr, len(out))
		for i := range out {
			outptrs[i] = out[i].Pointer() // points to the slice header's data
		}
		if err := enc.Write(in); err != nil {
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
	*/
}

func TestDecodingReaderWithZeros(t *testing.T) {
	// Gob, in its infinite cleverness, does not transmit zero values.
	// However, it apparently also does not zero out zero values in
	// structs that are reused. This requires special handling that is
	// tested here.
	type fields struct{ A, B, C int }
	var b bytes.Buffer
	in := []fields{{1, 2, 3}, {1, 0, 3}}
	enc := NewEncodingWriter(&b)
	if err := enc.Write(frame.Slices(in[0:1])); err != nil {
		t.Fatal(err)
	}
	if err := enc.Write(frame.Slices(in[1:2])); err != nil {
		t.Fatal(err)
	}

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

func TestDecodingReaderCorrupted(t *testing.T) {
	const N = 100
	col := make([]int, 100)
	for i := range col {
		col[i] = i
	}
	var b bytes.Buffer
	enc := NewEncodingWriter(&b)
	if err := enc.Write(frame.Slices(col, col, col)); err != nil {
		t.Fatal(err)
	}
	buf := func() []byte {
		p := b.Bytes()
		if len(p) == 0 {
			t.Fatal(p)
		}
		return append([]byte{}, p...)
	}
	rnd := rand.New(rand.NewSource(1234))
	var nintegrity int
	for i := 0; i < N; i++ {
		// First, check that it reads (valid); then corrupt a single bit
		// and make sure we get an error.
		p := buf()
		r := NewDecodingReader(bytes.NewReader(p))
		ctx := context.Background()
		var c1, c2, c3 []int
		if err := ReadAll(ctx, r, &c1, &c2, &c3); err != nil {
			t.Error(err)
			continue
		}
		i := rnd.Intn(len(p))
		p[i] ^= byte(1 << uint(rnd.Intn(8)))
		r = NewDecodingReader(bytes.NewReader(p))
		err := ReadAll(ctx, r, &c1, &c2, &c3)
		if err == nil {
			t.Error("got nil err")
			continue
		}
		// Depending on which bit gets flipped, we might end up with
		// a different decoding error.
		switch errors.Recover(err).Kind {
		default:
			t.Errorf("invalid error %v", err)
		case errors.Integrity:
			nintegrity++
		case errors.Other:
			switch {
			default:
				t.Errorf("invalid error %v", err)
			case strings.HasPrefix(err.Error(), "gob:"):
			case err.Error() == "extra data in buffer":
			case err.Error() == "unexpected EOF":
			}
		}
	}
	if nintegrity == 0 {
		t.Error("encountered no integrity errors")
	}
}

func TestDecodingSlices(t *testing.T) {
	// Gob will reuse slices during decoding if we're not careful.
	var b bytes.Buffer
	in := [][]string{{"a", "b"}, {"c", "d"}}
	enc := NewEncodingWriter(&b)
	if err := enc.Write(frame.Slices(in[0:1])); err != nil {
		t.Fatal(err)
	}
	if err := enc.Write(frame.Slices(in[1:2])); err != nil {
		t.Fatal(err)
	}

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

// TestScratchBufferGrowth verifies that decodingReader can buffer encoded
// frames of increasing length. Note that this is a very
// implementation-dependent test that verifies that scratch buffer resizing
// works correctly.
func TestScratchBufferGrowth(t *testing.T) {
	var (
		b   bytes.Buffer
		in0 = []int{0, 1}
		in1 = []int{2, 3, 4}
		enc = NewEncodingWriter(&b)
	)
	// Encode a 2-length frame followed by a 3-length frame. This will cause
	// the decoder to resize its scratch buffer.
	if err := enc.Write(frame.Slices(in0)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Write(frame.Slices(in1)); err != nil {
		t.Fatal(err)
	}

	r := NewDecodingReader(&b)
	// Read one row at a time to force the decodingReader to internally buffer
	// the decoded frames.
	f := frame.Make(slicetype.New(typeOfInt), 1, 1)
	var out []int
	for {
		n, err := r.Read(context.Background(), f)
		if err == EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out = append(out, f.Slice(0, n).Value(0).Interface().([]int)...)
	}
	if got, want := len(out), 5; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := out, append(in0, in1...); !reflect.DeepEqual(got, want) {
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
	enc := NewEncodingWriter(&b)
	for i := 0; i < N; i += Stride {
		j := i + Stride
		if j > N {
			j = N
		}
		args := make([]interface{}, len(cols))
		for k := range args {
			args[k] = reflect.ValueOf(cols[k]).Slice(i, j).Interface()
		}
		if err := enc.Write(frame.Slices(args...)); err != nil {
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

func TestSession(t *testing.T) {
	s := make(session)
	k1, k2 := frame.FreshKey(), frame.FreshKey()
	var x *int
	if !s.State(k1, &x) {
		t.Fatal("k1 not initialized")
	}
	var y *string
	if !s.State(k2, &y) {
		t.Fatal("k2 not initialized")
	}
	*y = "ok"

	x = nil
	if s.State(k1, &x) {
		t.Fatal("k1 initialized twice")
	}
	if got, want := *x, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	y = nil
	if s.State(k2, &y) {
		t.Fatal("k2 initialized twice")
	}
	if got, want := *y, "ok"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
