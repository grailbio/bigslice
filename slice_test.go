// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"text/tabwriter"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine/rpc"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/typecheck"
)

func init() {
	log.AddFlags() // so they can be used in tests
}

var (
	typeOfString  = reflect.TypeOf("")
	typeOfInt     = reflect.TypeOf(int(0))
	typeOfInt64   = reflect.TypeOf(int64(0))
	typeOfFloat64 = reflect.TypeOf(float64(0))
)

func sortColumns(columns []interface{}) {
	s := new(columnSlice)
	s.keys = columns[0].([]string)
	s.swappers = make([]func(i, j int), len(columns))
	for i := range columns {
		s.swappers[i] = reflect.Swapper(columns[i])
	}
	sort.Stable(s)
}

type columnSlice struct {
	keys     []string
	swappers []func(i, j int)
}

func (c columnSlice) Len() int           { return len(c.keys) }
func (c columnSlice) Less(i, j int) bool { return c.keys[i] < c.keys[j] }
func (c columnSlice) Swap(i, j int) {
	for _, swap := range c.swappers {
		swap(i, j)
	}
}

var executors = map[string]exec.Option{
	"Local":           exec.Local,
	"Bigmachine.Test": exec.Bigmachine(testsystem.New()),
}

// CanTolerateFailures reports whether the particular slice is able to
// recover from runtime failures.
//
// TODO(marius): remove once we can recover from tasks with combiner
// buffers.
func canTolerateFailures(slice bigslice.Slice) bool {
	if slice.Combiner() != nil {
		return false
	}
	for i := 0; i < slice.NumDep(); i++ {
		if !canTolerateFailures(slice.Dep(i)) {
			return false
		}
	}
	return true
}

func run(ctx context.Context, t *testing.T, slice bigslice.Slice) map[string]*sliceio.Scanner {
	results := make(map[string]*sliceio.Scanner)
	fn := bigslice.Func(func() bigslice.Slice { return slice })

	for name, opt := range executors {
		sess := exec.Start(opt)
		// TODO(marius): faster teardown in bigmachine so that we can call this here.
		// defer sess.Shutdown()
		res, err := sess.Run(ctx, fn)
		if err != nil {
			t.Errorf("executor %s error %v", name, err)
			continue
		}
		results[name] = res.Scan(ctx)
	}
	return results
}

func assertEqual(t *testing.T, slice bigslice.Slice, sort bool, expect ...interface{}) {
	if !testing.Short() {
		if canTolerateFailures(slice) {
			rpc.InjectFailures = true
			defer func() { rpc.InjectFailures = false }()
		} else {
			t.Logf("slice %s cannot recover from failures", bigslice.String(slice))
		}
	}

	t.Helper()
	if len(expect) == 0 {
		t.Fatal("need at least one column")
	}
	expectvs := make([]reflect.Value, len(expect))
	for i := range expect {
		expectvs[i] = reflect.ValueOf(expect[i])
		if expectvs[i].Kind() != reflect.Slice {
			t.Fatal("expect argument must be a slice")
		}
		if i > 1 && expectvs[i].Len() != expectvs[i-1].Len() {
			t.Fatal("expect argument length mismatch")
		}
	}
	for name, s := range run(context.Background(), t, slice) {
		t.Run(name, func(t *testing.T) {
			args := make([]interface{}, len(expect))
			for i := range args {
				// Make this one larger to make sure we exhaust the scanner.
				slice := reflect.MakeSlice(expectvs[i].Type(), expectvs[i].Len()+1, expectvs[i].Len()+1)
				args[i] = slice.Interface()
			}
			n, ok := s.Scanv(context.Background(), args...)
			if ok {
				t.Errorf("%s: long read (%d)", name, n)
			}
			if err := s.Err(); err != nil {
				t.Errorf("%s: %v", name, err)
				return
			}
			switch got, want := n, expectvs[0].Len(); {
			case got == want:
			case got < want:
				t.Errorf("%s: short result: got %v, want %v: got %v", name, got, want, args)
				return
			case want+1 == got:
				row := make([]string, len(args))
				for i := range row {
					row[i] = fmt.Sprint(reflect.ValueOf(args[i]).Index(got - 1).Interface())
				}
				t.Errorf("%s: extra values: %v", name, strings.Join(row, ","))
				n = want
			default:
				t.Errorf("%s: bad read: got %v, want %v", name, got, want)
				return
			}
			for i := range args {
				args[i] = reflect.ValueOf(args[i]).Slice(0, n).Interface()
			}
			if sort {
				if slice.Out(0).Kind() != reflect.String {
					t.Errorf("%s: can only sort string keys", name)
					return
				}
				sortColumns(args)
				sortColumns(expect)
			}
			if !reflect.DeepEqual(expect, args) {
				// Print as columns
				var b bytes.Buffer
				var tw tabwriter.Writer
				tw.Init(&b, 4, 4, 1, ' ', 0)
				for i := 0; i < n; i++ {
					var diff bool
					row := make([]string, len(args))
					for j := range row {
						got := reflect.ValueOf(args[j]).Index(i).Interface()
						want := reflect.ValueOf(expect[j]).Index(i).Interface()
						if !reflect.DeepEqual(got, want) {
							diff = true
							row[j] = fmt.Sprintf("%v->%v", want, got)
						} else {
							row[j] = fmt.Sprint(got)
						}
					}
					if diff {
						fmt.Fprintf(&tw, "[%d] %s\n", i, strings.Join(row, "\t"))
					}
				}
				tw.Flush()
				t.Errorf("%s: result mismatch:\n%s", name, b.String())
			}
		})
	}
}

func expectTypeError(t *testing.T, message string, fn func()) {
	t.Helper()
	typecheck.TestCalldepth = 2
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		t.Fatal("runtime.Caller error")
	}
	defer func() {
		t.Helper()
		typecheck.TestCalldepth = 0
		e := recover()
		if e == nil {
			t.Fatal("expected error")
		}
		err, ok := e.(*typecheck.Error)
		if !ok {
			t.Fatalf("expected typeError, got %T", e)
		}
		if got, want := err.File, file; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := err.Line, line; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := err.Err.Error(), message; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}()
	fn()
}

func TestConst(t *testing.T) {
	const N = 10000
	fz := fuzz.New()
	fz.NilChance(0)
	fz.NumElements(N, N)
	var (
		col1 []string
		col2 []int
	)
	fz.Fuzz(&col1)
	fz.Fuzz(&col2)
	for nshards := 1; nshards < 20; nshards++ {
		slice := bigslice.Const(nshards, col1, col2)
		assertEqual(t, slice, true, col1, col2)
	}
}

func TestConstError(t *testing.T) {
	expectTypeError(t, "const: invalid slice inputs", func() { bigslice.Const(1, 123) })
}

func TestReaderFunc(t *testing.T) {
	const (
		N      = 10000
		Nshard = 10
	)
	type state struct {
		*fuzz.Fuzzer
		total int
	}
	slice := bigslice.ReaderFunc(Nshard, func(shard int, state *state, strings []string, ints []int) (n int, err error) {
		// The input should be zerod by bigslice.
		var nnonzero int
		for i := range strings {
			if strings[i] != "" || ints[i] != 0 {
				nnonzero++
			}
		}
		if nnonzero > 0 {
			t.Errorf("%d (of %d) nonzero rows", nnonzero, len(strings))
		}
		if state.Fuzzer == nil {
			state.Fuzzer = fuzz.New()
		}
		state.NumElements(1, len(strings))
		var (
			fstrings []string
			fints    []int
		)
		state.Fuzz(&fstrings)
		state.Fuzz(&fints)
		n = copy(strings, fstrings)
		m := copy(ints, fints)
		if m < n {
			n = m
		}
		state.total += n
		if state.total >= N {
			return n - (state.total - N), sliceio.EOF
		}
		return n, nil
	})
	// Map everything to the same key so we can count them.
	slice = bigslice.Map(slice, func(s string, i int) (key string, count int) { return "", 1 })
	slice = bigslice.Fold(slice, func(a, e int) int { return a + e })
	assertEqual(t, slice, false, []string{""}, []int{N * Nshard})
}

func TestReaderFuncError(t *testing.T) {
	expectTypeError(t, "readerfunc: invalid reader function type func()", func() { bigslice.ReaderFunc(1, func() {}) })
	expectTypeError(t, "readerfunc: invalid reader function type string", func() { bigslice.ReaderFunc(1, "invalid") })
	expectTypeError(t, "readerfunc: invalid reader function type func(string, string, []int) (int, error)", func() { bigslice.ReaderFunc(1, func(shard string, state string, x []int) (int, error) { panic("") }) })
	expectTypeError(t, "readerfunc: function func(int, string, []int) error does not return (int, error)", func() { bigslice.ReaderFunc(1, func(shard int, state string, x []int) error { panic("") }) })
	expectTypeError(t, "readerfunc: invalid reader function type func(int, string) (int, error)", func() { bigslice.ReaderFunc(1, func(shard int, state string) (int, error) { panic("") }) })

}

func TestMap(t *testing.T) {
	const N = 100000
	input := make([]int, N)
	output := make([]string, N)
	for i := range input {
		input[i] = i
		output[i] = fmt.Sprint(i)
	}
	slice := bigslice.Const(1, input)
	slice = bigslice.Map(slice, func(i int) string { return fmt.Sprint(i) })
	assertEqual(t, slice, false, output)
}

func TestMapError(t *testing.T) {
	input := bigslice.Const(1, []string{"x", "y"})
	expectTypeError(t, "map: invalid map function int", func() { bigslice.Map(input, 123) })
	expectTypeError(t, "map: function func(int) string does not match input slice type slice[1]string", func() { bigslice.Map(input, func(x int) string { return "" }) })
	expectTypeError(t, "map: function func(int, int) string does not match input slice type slice[1]string", func() { bigslice.Map(input, func(x, y int) string { return "" }) })
	expectTypeError(t, "map: need at least one output column", func() { bigslice.Map(input, func(x string) {}) })
}

func TestFilter(t *testing.T) {
	const N = 100000
	input := make([]int, N)
	output := make([]int, N/2)
	for i := range input {
		input[i] = i
		if i%2 == 0 {
			output[i/2] = i
		}
	}
	slice := bigslice.Const(N/1000, input)
	slice = bigslice.Filter(slice, func(i int) bool { return i%2 == 0 })
	assertEqual(t, slice, false, output)

	slice = bigslice.Const(1, input)
	slice = bigslice.Filter(slice, func(i int) bool { return false })
	assertEqual(t, slice, false, []int{})

	slice = bigslice.Const(1, input)
	slice = bigslice.Filter(slice, func(i int) bool {
		switch i {
		case N / 4, N / 2, 3 * N / 4:
			return true
		default:
			return false
		}
	})
	assertEqual(t, slice, false, []int{N / 4, N / 2, 3 * N / 4})
}

func TestFilterError(t *testing.T) {
	input := bigslice.Const(1, []string{"x", "y"})
	expectTypeError(t, "filter: invalid predicate function int", func() { bigslice.Filter(input, 123) })
	expectTypeError(t, "filter: function func(int) bool does not match input slice type slice[1]string", func() { bigslice.Filter(input, func(x int) bool { return false }) })
	expectTypeError(t, "filter: function func(int, int) string does not match input slice type slice[1]string", func() { bigslice.Filter(input, func(x, y int) string { return "" }) })
	expectTypeError(t, "filter: predicate must return a single boolean value", func() { bigslice.Filter(input, func(x string) {}) })
	expectTypeError(t, "filter: predicate must return a single boolean value", func() { bigslice.Filter(input, func(x string) int { return 0 }) })
	expectTypeError(t, "filter: predicate must return a single boolean value", func() { bigslice.Filter(input, func(x string) (bool, int) { return false, 0 }) })
}

func TestFlatmap(t *testing.T) {
	slice := bigslice.Const(2, []string{"x,x", "y,y,y", "z", "", "x"})
	slice = bigslice.Flatmap(slice, func(s string) []string {
		if s == "" {
			return nil
		}
		return strings.Split(s, ",")
	})
	assertEqual(t, slice, true, []string{"x", "x", "x", "y", "y", "y", "z"})

	// Multiple columns
	slice = bigslice.Flatmap(slice, func(s string) ([]string, []int) {
		return []string{s}, []int{len(s)}
	})
	assertEqual(t, slice, true,
		[]string{"x", "x", "x", "y", "y", "y", "z"},
		[]int{1, 1, 1, 1, 1, 1, 1},
	)

	// Filter everything
	slice = bigslice.Flatmap(slice, func(s string, i int) []string {
		return nil
	})
	assertEqual(t, slice, true, []string{})

	// Partial filter
	slice = bigslice.Const(1, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	slice = bigslice.Flatmap(slice, func(i int) []int {
		if i%2 == 0 {
			return []int{i}
		}
		return nil
	})
	assertEqual(t, slice, false, []int{0, 2, 4, 6, 8, 10})

	// Large slices
	input := make([]string, 1024*10)
	for i := range input {
		input[i] = fmt.Sprint(i)
	}
	slice = bigslice.Const(5, input)
	slice = bigslice.Flatmap(slice, func(s string) []string {
		switch s {
		case "1024":
			return []string{s}
		case "5000":
			return []string{s}
		default:
			return nil
		}
	})
	assertEqual(t, slice, true, []string{"1024", "5000"})
}

func TestFlatmapBuffered(t *testing.T) {
	zeros := make([]int, 1025)
	slice := bigslice.Const(1, []int{0})
	slice = bigslice.Flatmap(slice, func(i int) []int {
		return zeros
	})
	// Drive it manually:
	assertEqual(t, slice, false, zeros)
}

func TestFlatmapError(t *testing.T) {
	input := bigslice.Const(1, []int{1, 2, 3})
	expectTypeError(t, "flatmap: invalid flatmap function int", func() { bigslice.Flatmap(input, 123) })
	expectTypeError(t, "flatmap: flatmap function func(string) []int does not match input slice type slice[1]int", func() { bigslice.Flatmap(input, func(s string) []int { return nil }) })
	expectTypeError(t, "flatmap: flatmap function func(int) int is not vectorized", func() { bigslice.Flatmap(input, func(i int) int { return 0 }) })
	expectTypeError(t, "flatmap: flatmap function func(int, int) []int does not match input slice type slice[1]int", func() { bigslice.Flatmap(input, func(i, j int) []int { return nil }) })

}

func TestFold(t *testing.T) {
	const N = 10000
	fz := fuzz.New()
	fz.NilChance(0)
	fz.NumElements(N/2, N/2)
	var (
		keys   []string
		values []int
	)
	fz.Fuzz(&keys)
	fz.Fuzz(&values)
	keys = append(keys, keys...)
	values = append(values, values...)
	slice := bigslice.Const(N/1000, keys, values)
	slice = bigslice.Fold(slice, func(a, e int) int { return a + e })

	expect := make(map[string]int)
	for i, key := range keys {
		expect[key] += values[i]
	}
	var (
		expectKeys   []string
		expectValues []int
	)
	for key, value := range expect {
		expectKeys = append(expectKeys, key)
		expectValues = append(expectValues, value)
	}
	assertEqual(t, slice, true, expectKeys, expectValues)

	// Make sure we can partition other element types also.
	slice = bigslice.Const(N/1000, values, keys)
	slice = bigslice.Fold(slice, func(a int, e string) int { return a + len(e) })
	slice = bigslice.Map(slice, func(key, count int) (int, int) { return 0, count })
	slice = bigslice.Fold(slice, func(a, e int) int { return a + e })
	var totalSize int
	for _, key := range keys {
		totalSize += len(key)
	}
	assertEqual(t, slice, false, []int{0}, []int{totalSize})
}

func TestFoldError(t *testing.T) {
	input := bigslice.Const(1, []int{1, 2, 3})
	floatInput := bigslice.Map(input, func(x int) (float64, int) { return 0, 0 })
	intInput := bigslice.Map(input, func(x int) (int, int) { return 0, 0 })
	expectTypeError(t, "fold: key type float64 cannot be accumulated", func() { bigslice.Fold(floatInput, func(x int) int { return 0 }) })
	expectTypeError(t, "Fold can be applied only for slices with at least two columns; got 1", func() { bigslice.Fold(input, func(x int) int { return 0 }) })
	expectTypeError(t, "fold: expected func(acc, t2, t3, ..., tn), got func(int) int", func() { bigslice.Fold(intInput, func(x int) int { return 0 }) })
	expectTypeError(t, "fold: expected func(acc, t2, t3, ..., tn), got func(int, int) string", func() { bigslice.Fold(intInput, func(a, x int) string { return "" }) })
	expectTypeError(t, "fold: fold functions must return exactly one value", func() { bigslice.Fold(intInput, func(a, x int) (int, int) { return 0, 0 }) })
	expectTypeError(t, "fold: expected func(acc, t2, t3, ..., tn), got func(int, string) int", func() { bigslice.Fold(intInput, func(a int, x string) int { return 0 }) })
}

func TestHead(t *testing.T) {
	slice := bigslice.Head(bigslice.Const(2, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}), 2)
	assertEqual(t, slice, false, []int{1, 2, 7, 8})
}

func TestScan(t *testing.T) {
	const (
		N      = 10000
		Nshard = 10
	)
	input := make([]int, N)
	for i := range input {
		input[i] = i
	}
	var mu sync.Mutex
	output := make([]int, N)
	shards := make([]int, Nshard)
	slice := bigslice.Const(Nshard, input)
	slice = bigslice.Scan(slice, func(shard int, scan *sliceio.Scanner) error {
		mu.Lock()
		defer mu.Unlock()
		shards[shard]++
		var elem int
		ctx := context.Background()
		for scan.Scan(ctx, &elem) {
			output[elem]++
		}
		return scan.Err()
	})
	n := len(run(context.Background(), t, slice))
	for i, got := range output {
		if want := n; got != want {
			t.Errorf("wrong count for output %d, got %v, want %v", i, got, want)
		}
	}
	for i, got := range shards {
		if want := n; got != want {
			t.Errorf("wrong count for shard %d, got %v, want %v", i, got, want)
		}
	}
}

func TestPanic(t *testing.T) {
	slice := bigslice.Const(1, []int{1, 2, 3})
	slice = bigslice.Map(slice, func(i int) int {
		panic(i)
	})
	fn := bigslice.Func(func() bigslice.Slice { return slice })
	ctx := context.Background()
	for name, opt := range executors {
		sess := exec.Start(opt)
		// TODO(marius): faster teardown in bigmachine so that we can call this here.
		// defer sess.Shutdown()
		_, err := sess.Run(ctx, fn)
		if err == nil {
			t.Errorf("executor %s: expected error", name)
			continue
		}
		if msg := err.Error(); !strings.Contains(msg, "panic while evaluating slice") {
			t.Errorf("wrong error message %q", msg)
		}
	}
}
