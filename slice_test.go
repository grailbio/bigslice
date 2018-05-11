// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

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
	"github.com/grailbio/bigmachine/testsystem"
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

var executors = map[string]Option{
	"Local":           Local,
	"Bigmachine.Test": Bigmachine(testsystem.New()),
}

func run(ctx context.Context, t *testing.T, slice Slice) map[string]*Scanner {
	results := make(map[string]*Scanner)
	fn := Func(func() Slice { return slice })

	for name, opt := range executors {
		sess := Start(opt)
		// TODO(marius): faster teardown in bigmachine so that we can call this here.
		// defer sess.Shutdown()
		if err := sess.Run(ctx, fn); err != nil {
			t.Errorf("executor %s error %v", name, err)
			continue
		}
		// TODO(marius): This is a bit of a hack, as we're relying on the
		// fact that we're not doing concurrent invocations. Fix this by
		// first-class support for session readers.
		tasks := sess.tasks[fn.Invocation().Index-1]
		if tasks == nil {
			t.Fatal("tasks == nil")
		}
		scan := &Scanner{
			out:     ColumnTypes(slice),
			readers: make([]Reader, len(tasks)),
		}
		for i := range scan.readers {
			scan.readers[i] = sess.executor.Reader(ctx, tasks[i], 0)
		}
		results[name] = scan
	}
	return results
}

func assertEqual(t *testing.T, slice Slice, sort bool, expect ...interface{}) {
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
			continue
		}
		switch got, want := n, expectvs[0].Len(); {
		case got == want:
		case got < want:
			t.Errorf("%s: short result: got %v, want %v: got %v", name, got, want, args)
			continue
		case want+1 == got:
			row := make([]string, len(args))
			for i := range row {
				row[i] = fmt.Sprint(reflect.ValueOf(args[i]).Index(got - 1).Interface())
			}
			t.Errorf("%s: extra values: %v", name, strings.Join(row, ","))
			n = want
		default:
			t.Errorf("%s: bad read: got %v, want %v", name, got, want)
			continue
		}
		for i := range args {
			args[i] = reflect.ValueOf(args[i]).Slice(0, n).Interface()
		}
		if sort {
			if slice.Out(0).Kind() != reflect.String {
				t.Errorf("%s: can only sort string keys", name)
				continue
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
	}
}

func expectTypeError(t *testing.T, message string, fn func()) {
	t.Helper()
	testCalldepth = 2
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		t.Fatal("runtime.Caller error")
	}
	defer func() {
		testCalldepth = 0
		e := recover()
		if e == nil {
			t.Fatal("expected error")
		}
		err, ok := e.(typeError)
		if !ok {
			t.Fatalf("expected typeError, got %T", e)
		}
		if got, want := err.file, file; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := err.line, line; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := err.err.Error(), message; got != want {
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
		slice := Const(nshards, col1, col2)
		assertEqual(t, slice, true, col1, col2)
	}
}

func TestConstError(t *testing.T) {
	expectTypeError(t, "invalid column 0: expected slice, got int", func() { Const(1, 123) })
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
	slice := ReaderFunc(Nshard, func(shard int, state *state, strings []string, ints []int) (n int, err error) {
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
			return n - (state.total - N), EOF
		}
		return n, nil
	})
	// Map everything to the same key so we can count them.
	slice = Map(slice, func(s string, i int) (key string, count int) { return "", 1 })
	slice = Fold(slice, func(a, e int) int { return a + e })
	assertEqual(t, slice, false, []string{""}, []int{N * Nshard})
}

func TestReaderFuncError(t *testing.T) {
	expectTypeError(t, "reader functions need at least a shard argument, a state, and at least one column", func() { ReaderFunc(1, func() {}) })
	expectTypeError(t, "expected a function, got string", func() { ReaderFunc(1, "invalid") })
	expectTypeError(t, "reader functions must have an integer shard number as its first argument", func() { ReaderFunc(1, func(shard string, state string, x []int) (int, error) { panic("") }) })
	expectTypeError(t, "reader function must return (int, error), got func(int, string, []int) error", func() { ReaderFunc(1, func(shard int, state string, x []int) error { panic("") }) })
	expectTypeError(t, "reader functions need at least a shard argument, a state, and at least one column", func() { ReaderFunc(1, func(shard int, state string) (int, error) { panic("") }) })

}

func TestMap(t *testing.T) {
	const N = 100000
	input := make([]int, N)
	output := make([]string, N)
	for i := range input {
		input[i] = i
		output[i] = fmt.Sprint(i)
	}
	slice := Const(1, input)
	slice = Map(slice, func(i int) string { return fmt.Sprint(i) })
	assertEqual(t, slice, false, output)
}

func TestMapError(t *testing.T) {
	input := Const(1, []string{"x", "y"})
	expectTypeError(t, "expected a function, got int", func() { Map(input, 123) })
	expectTypeError(t, "expected type string for argument 0, got int", func() { Map(input, func(x int) string { return "" }) })
	expectTypeError(t, "expected 1 argument, got 2", func() { Map(input, func(x, y int) string { return "" }) })
	expectTypeError(t, "map functions need at least one output column", func() { Map(input, func(x string) {}) })
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
	slice := Const(N/1000, input)
	slice = Filter(slice, func(i int) bool { return i%2 == 0 })
	assertEqual(t, slice, false, output)

	slice = Const(1, input)
	slice = Filter(slice, func(i int) bool { return false })
	assertEqual(t, slice, false, []int{})

	slice = Const(1, input)
	slice = Filter(slice, func(i int) bool {
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
	input := Const(1, []string{"x", "y"})
	expectTypeError(t, "expected a function, got int", func() { Filter(input, 123) })
	expectTypeError(t, "wrong type for argument 0: expected string, not int", func() { Filter(input, func(x int) bool { return false }) })
	expectTypeError(t, "expected 1 arguments, got 2", func() { Filter(input, func(x, y int) string { return "" }) })
	expectTypeError(t, "predicates should return a single boolean value", func() { Filter(input, func(x string) {}) })
	expectTypeError(t, "predicates should return a single boolean value", func() { Filter(input, func(x string) int { return 0 }) })
	expectTypeError(t, "predicates should return a single boolean value", func() { Filter(input, func(x string) (bool, int) { return false, 0 }) })
}

func TestFlatmap(t *testing.T) {
	slice := Const(2, []string{"x,x", "y,y,y", "z", "", "x"})
	slice = Flatmap(slice, func(s string) []string {
		if s == "" {
			return nil
		}
		return strings.Split(s, ",")
	})
	assertEqual(t, slice, true, []string{"x", "x", "x", "y", "y", "y", "z"})

	// Multiple columns
	slice = Flatmap(slice, func(s string) ([]string, []int) {
		return []string{s}, []int{len(s)}
	})
	assertEqual(t, slice, true,
		[]string{"x", "x", "x", "y", "y", "y", "z"},
		[]int{1, 1, 1, 1, 1, 1, 1},
	)

	// Filter everything
	slice = Flatmap(slice, func(s string, i int) []string {
		return nil
	})
	assertEqual(t, slice, true, []string{})

	// Partial filter
	slice = Const(1, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	slice = Flatmap(slice, func(i int) []int {
		if i%2 == 0 {
			return []int{i}
		}
		return nil
	})
	assertEqual(t, slice, false, []int{0, 2, 4, 6, 8, 10})

	// Large slices
	input := make([]string, defaultChunksize*10)
	for i := range input {
		input[i] = fmt.Sprint(i)
	}
	slice = Const(5, input)
	slice = Flatmap(slice, func(s string) []string {
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

func TestFlatmapError(t *testing.T) {
	input := Const(1, []int{1, 2, 3})
	expectTypeError(t, "expected a function, got int", func() { Flatmap(input, 123) })
	expectTypeError(t, "expected type int for argument 0, got string", func() { Flatmap(input, func(s string) []int { return nil }) })
	expectTypeError(t, "output argument 0 must be a slice, not int", func() { Flatmap(input, func(i int) int { return 0 }) })
	expectTypeError(t, "expected 1 arguments, got 2", func() { Flatmap(input, func(i, j int) []int { return nil }) })

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
	slice := Const(N/1000, keys, values)
	slice = Fold(slice, func(a, e int) int { return a + e })

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
	slice = Const(N/1000, values, keys)
	slice = Fold(slice, func(a int, e string) int { return a + len(e) })
	slice = Map(slice, func(key, count int) (int, int) { return 0, count })
	slice = Fold(slice, func(a, e int) int { return a + e })
	var totalSize int
	for _, key := range keys {
		totalSize += len(key)
	}
	assertEqual(t, slice, false, []int{0}, []int{totalSize})
}

func TestFoldError(t *testing.T) {
	input := Const(1, []int{1, 2, 3})
	floatInput := Map(input, func(x int) (float64, int) { return 0, 0 })
	intInput := Map(input, func(x int) (int, int) { return 0, 0 })
	expectTypeError(t, "key type float64 cannot be accumulated", func() { Fold(floatInput, func(x int) int { return 0 }) })
	expectTypeError(t, "Fold can be applied only for slices with at least two columns; got 1", func() { Fold(input, func(x int) int { return 0 }) })
	expectTypeError(t, "expected 2 arguments, got 1", func() { Fold(intInput, func(x int) int { return 0 }) })
	expectTypeError(t, "expected output type int, got string", func() { Fold(intInput, func(a, x int) string { return "" }) })
	expectTypeError(t, "accumulators must return a single value, not 2", func() { Fold(intInput, func(a, x int) (int, int) { return 0, 0 }) })
	expectTypeError(t, "wrong type for argument 1: expected int, not string", func() { Fold(intInput, func(a int, x string) int { return 0 }) })
}

func TestHead(t *testing.T) {
	slice := Head(Const(2, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}), 2)
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
	slice := Const(Nshard, input)
	slice = Scan(slice, func(shard int, scan *Scanner) error {
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
