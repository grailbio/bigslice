// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package frame

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"unsafe"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/slicetype"
)

var (
	typeOfString = reflect.TypeOf("")
	typeOfInt    = reflect.TypeOf(0)

	testType = slicetype.New(typeOfString, typeOfInt)
)

func assertEqual(t *testing.T, f, g Frame) {
	t.Helper()
	if got, want := f.Len(), g.Len(); got != want {
		t.Fatalf("length mismatch: got %v, want %v", got, want)
	}
	if got, want := f.NumOut(), g.NumOut(); got != want {
		t.Fatalf("column length mismatch: got %v, want %v", got, want)
	}
	// Test two different ways just to stress the system:
	for i := 0; i < f.NumOut(); i++ {
		if got, want := f.Interface(i), g.Interface(i); !reflect.DeepEqual(got, want) {
			t.Errorf("column %d not equal: got %v, want %v", i, got, want)
		}
	}
	for i := 0; i < f.NumOut(); i++ {
		for j := 0; j < f.Len(); j++ {
			if got, want := f.Index(i, j).Interface(), g.Index(i, j).Interface(); !reflect.DeepEqual(got, want) {
				t.Errorf("value %d,%d not equal: got %v, want %v", i, j, got, want)
			}
		}
	}
}

func assertZeros(t *testing.T, f Frame) {
	t.Helper()
	for i := 0; i < f.NumOut(); i++ {
		for j := 0; j < f.Len(); j++ {
			if got, want := f.Index(i, j).Interface(), reflect.Zero(f.Out(i)).Interface(); !reflect.DeepEqual(got, want) {
				t.Errorf("not zero %d,%d: got %v, want %v", i, j, got, want)
			}
		}
	}
}

func fuzzFrame(min int) Frame {
	fz := fuzz.New()
	fz.NilChance(0)
	fz.NumElements(min, min*10)
	var (
		ints []int
		strs []string
	)
	fz.Fuzz(&ints)
	fz.Fuzz(&strs)
	if len(ints) < len(strs) {
		strs = strs[:len(ints)]
	} else {
		ints = ints[:len(strs)]
	}
	return Slices(ints, strs)
}

func TestSliceRetain(t *testing.T) {
	typ := slicetype.New(typeOfString, typeOfInt)
	frame := Make(typ, 1024, 1024)
	f2 := frame.Slice(0, 200)
	x := f2.Interface(1).([]int)
	if got, want := len(x), 200; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	frame = Frame{}
	f2 = Frame{}
	runtime.GC()
	if got, want := len(x), 200; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSlice(t *testing.T) {
	f := fuzzFrame(10)
	assertEqual(t, f.Slice(1, 6).Slice(1, 2), f.Slice(2, 3))
	assertEqual(t, f, f.Slice(0, f.Len()))
	assertEqual(t, f.Slice(0, f.Cap()), f.Slice(0, f.Cap()))

	g := f.Slice(0, 0)
	if got, want := g.Len(), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assertEqual(t, f.Slice(0, 0).Slice(0, f.Len()), f)
}

func TestCopy(t *testing.T) {
	f := fuzzFrame(100)
	g := Make(f, f.Len(), f.Len())
	if got, want := Copy(g.Slice(0, 1), f), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assertEqual(t, f.Slice(0, 1), g.Slice(0, 1))
	if got, want := Copy(g.Slice(50, g.Len()), f), f.Len()-50; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assertEqual(t, g.Slice(50, f.Len()-50), f.Slice(0, f.Len()-100))
}

func TestAppendFrame(t *testing.T) {
	f := fuzzFrame(100)
	g := AppendFrame(f, f)
	if got, want := g.Len(), f.Len()*2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assertEqual(t, f, g.Slice(0, f.Len()))
	assertEqual(t, f, g.Slice(f.Len(), g.Len()))
	g = fuzzFrame(1000)
	assertEqual(t, AppendFrame(f, g), AppendFrame(f, g))
}

func TestGrow(t *testing.T) {
	f := fuzzFrame(100)
	g := f.Grow(f.Len())
	if got, want := g.Len(), f.Len()*2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assertEqual(t, g.Slice(0, f.Len()), f)
	assertZeros(t, g.Slice(f.Len(), g.Len()))
}

func TestSwap(t *testing.T) {
	f := fuzzFrame(100)
	g := Make(f, f.Len(), f.Len())
	Copy(g, f)
	for i := 1; i < f.Len(); i++ {
		f.Swap(i-1, i)
	}
	assertEqual(t, g.Slice(0, 1), f.Slice(f.Len()-1, f.Len()))
	assertEqual(t, g.Slice(1, g.Len()), f.Slice(0, f.Len()-1))
}

func TestClear(t *testing.T) {
	f := fuzzFrame(100)
	f.Slice(1, 50).Clear()
	g := Make(f, f.Len(), f.Len())
	assertEqual(t, f.Slice(1, 50), g.Slice(0, 49))
}

func TestClearx(t *testing.T) {
	type struct1 struct {
		a, b, c int
	}
	type struct2 struct {
		b [10000]byte
	}
	type struct3 struct {
		struct1
		two *struct2
		ok  string
	}
	slices := []interface{}{
		new([]int),
		new([]int64),
		new([]struct1),
		new([]struct2),
		new([]struct3),
		new([]string),
		new([]byte),
	}
	fz := fuzz.New()
	fz.NilChance(0)
	fz.NumElements(100, 1000)
	for _, slicep := range slices {
		fz.Fuzz(slicep)
		slice := reflect.Indirect(reflect.ValueOf(slicep)).Interface()
		frame := Slices(slice)
		frame.Clear()
		slicev := frame.Value(0)
		zero := reflect.Zero(slicev.Type().Elem()).Interface()
		for i := 0; i < slicev.Len(); i++ {
			if !reflect.DeepEqual(zero, slicev.Index(i).Interface()) {
				t.Errorf("index %d of slice %v not zero", i, slicep)
			}
		}
	}
}

func TestUnsafeIndexAddr(t *testing.T) {
	f := fuzzFrame(100)
	c0 := f.Interface(0).([]int)
	c1 := f.Interface(1).([]string)
	for i := 0; i < f.Len(); i++ {
		v0 := *(*int)(unsafe.Pointer(f.UnsafeIndexAddr(0, i)))
		if got, want := v0, c0[i]; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		v1 := *(*string)(unsafe.Pointer(f.UnsafeIndexAddr(1, i)))
		if got, want := v1, c1[i]; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestSort(t *testing.T) {
	f := fuzzFrame(1000)
	if sort.IsSorted(f) {
		t.Fatal("unlikely")
	}
	sort.Sort(f)
	if !sort.IsSorted(f) {
		t.Error("failed to sort")
	}
}

var copySizes = [...]int{8, 32, 256, 1024, 65536}

func benchmarkCopy(b *testing.B, copy func(dst, src Frame, i0, i1 int)) {
	b.Helper()
	for _, size := range copySizes {
		src, dst := Make(testType, size, size), Make(testType, size, size)
		for _, len := range []int{size / 2, size} {
			bench := func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(int(typeOfInt.Size())*size + int(typeOfString.Size())*size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					copy(dst, src, 0, len)
				}
			}
			name := fmt.Sprintf("size=%d,len=%d", size, len)
			b.Run(name, bench)
		}
	}
}

func BenchmarkCopy(b *testing.B) {
	benchmarkCopy(b, func(dst, src Frame, i0, i1 int) {
		Copy(dst.Slice(i0, i1), src)
	})
}

func BenchmarkReflectCopy(b *testing.B) {
	benchmarkCopy(b, func(dst, src Frame, i0, i1 int) {
		for i := 0; i < dst.NumOut(); i++ {
			reflect.Copy(dst.Value(i).Slice(i0, i1), src.Value(i))
		}
	})
}

func benchmarkAssign(b *testing.B, assign func(dst, src Frame)) {
	b.Helper()
	const N = 1024
	src, dst := Make(testType, N, N), Make(testType, N, N)
	b.ReportAllocs()
	b.SetBytes(int64(typeOfInt.Size()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		assign(dst, src)
	}
}

func BenchmarkUnsafeAssign(b *testing.B) {
	benchmarkAssign(b, func(dst, src Frame) {
		*(*int)(unsafe.Pointer(dst.UnsafeIndexAddr(0, 0))) =
			*(*int)(unsafe.Pointer(src.UnsafeIndexAddr(0, 0)))
	})
}

func BenchmarkAssign(b *testing.B) {
	benchmarkAssign(b, func(dst, src Frame) {
		dst.Index(0, 0).Set(src.Index(0, 0))
	})
}

const Nsort = 2 << 19

func makeInts(n int) (data []int, shuffle func()) {
	rnd := rand.New(rand.NewSource(int64(n)))
	slice := rnd.Perm(n)
	return slice, func() {
		rnd.Shuffle(n, func(i, j int) { slice[i], slice[j] = slice[j], slice[i] })
	}
}

func BenchmarkSort(b *testing.B) {
	slice, shuffle := makeInts(Nsort)
	f := Slices(slice)
	b.ReportAllocs()
	b.ResetTimer()
	_ = shuffle

	for i := 0; i < b.N; i++ {
		sort.Sort(f)
		shuffle()
	}
}

func BenchmarkSortSlice(b *testing.B) {
	slice, shuffle := makeInts(Nsort)
	b.ReportAllocs()
	b.ResetTimer()
	_ = shuffle
	for i := 0; i < b.N; i++ {
		sort.Slice(slice, func(i, j int) bool {
			return slice[i] < slice[j]
		})
		shuffle()
	}
}

func BenchmarkSortFrameSlice(b *testing.B) {
	slice, shuffle := makeInts(Nsort)
	f := Slices(slice)
	b.ReportAllocs()
	b.ResetTimer()
	_ = shuffle
	for i := 0; i < b.N; i++ {
		sort.Slice(slice, func(i, j int) bool { return f.data[0].ops.Less(i, j) })
		shuffle()
	}
}
