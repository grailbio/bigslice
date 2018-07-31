// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/typecheck"
)

// testCalldepth is used by tests to verify the correctness of
// caller attribution in error messages.
var testCalldepth = 0

var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

// DefaultChunkSize is the default size used for IO vectors throughout bigslice.
const defaultChunksize = 1024

var errTypeError = errors.New("type error")

// A Dep is a Slice dependency. Deps comprise a slice and a boolean
// flag determining whether this is represents a shuffle dependency.
// Shuffle dependencies must perform a data shuffle step: the
// dependency must partition its output according to the Slice's
// partitioner, and, when the dependent Slice is computed, the
// evaluator must pass in Readers that read a single partition from
// all dependent shards.
type Dep struct {
	Slice
	Shuffle bool
	// Expand indicates that each shard of a shuffle dependency (i.e.,
	// all the shards of a given partition) should be expanded (i.e.,
	// not merged) when handed to the slice implementation. This is to
	// support merge-sorting of shards of the same partition.
	Expand bool
}

// ShardType indicates the type of sharding used by a Slice.
type ShardType int

const (
	// HashShard Slices are partitioned by an (unspecified)
	// hash of an record. That is, the same record should
	// be assigned a stable shard number.
	HashShard ShardType = iota
	// RangeShard Slices are partitioned by the range of a key. The key
	// is always the first column of the slice.
	RangeShard
)

// A Slice is a shardable, ordered dataset. Each slice consists of
// zero or more columns of data distributed over one or  more shards.
// Slices may declare dependencies on other slices from which it is
// computed. In order to compute a slice, its dependencies must first
// be computed, and their resulting Readers are passed to a Slice's
// Reader method.
//
// Since Go does not support generic typing, Slice combinators
// perform their own dynamic type checking. Schematically we write
// the n-ary slice with types t1, t2, ..., tn as Slice<t1, t2, ..., tn>.
type Slice interface {
	// NumOut returns the number of columns in this slice.
	NumOut() int
	// Out returns the data type of a column.
	Out(column int) reflect.Type
	// Op is a descriptive name of the operation that this Slice
	// represents.
	Op() string

	// NumShard returns the number of shards in this Slice.
	NumShard() int
	// ShardType returns the sharding type of this Slice.
	ShardType() ShardType

	// NumDep returns the number of dependencies of this Slice.
	NumDep() int
	// Dep returns the i'th dependency for this Slice.
	Dep(i int) Dep

	// Combiner is an optional function that is used to combine multiple
	// values with the same key from the slice's output. No combination
	// is performed if nil.
	Combiner() *reflect.Value

	// Reader returns a Reader for a shard of this Slice. The reader
	// itself computes the shard's values on demand. The caller must
	// provide Readers for all of this shard's dependencies, constructed
	// according to the dependency type (see Dep).
	Reader(shard int, deps []sliceio.Reader) sliceio.Reader
}

type constSlice struct {
	slicetype.Type
	frame  frame.Frame
	nshard int
}

// Const returns a Slice representing the provided value. Each column
// of the Slice should be provided as a Go slice of the column's
// type. The value is split into nshard shards.
func Const(nshard int, columns ...interface{}) Slice {
	if len(columns) == 0 {
		typecheck.Panic(1, "const: must have at least one column")
	}
	s := new(constSlice)
	s.nshard = nshard
	if s.nshard < 1 {
		typecheck.Panic(1, "const: shard must be >= 1")
	}
	var ok bool
	s.Type, ok = typecheck.Slices(columns...)
	if !ok {
		typecheck.Panic(1, "const: invalid slice inputs")
	}
	// TODO(marius): convert panic to a typecheck panic
	s.frame = frame.Slices(columns...)
	return s
}

func (*constSlice) Op() string               { return "const" }
func (s *constSlice) NumShard() int          { return s.nshard }
func (*constSlice) ShardType() ShardType     { return HashShard }
func (*constSlice) NumDep() int              { return 0 }
func (*constSlice) Dep(i int) Dep            { panic("no deps") }
func (*constSlice) Combiner() *reflect.Value { return nil }

type constReader struct {
	op    *constSlice
	frame frame.Frame
	shard int
}

func (s *constReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	if !slicetype.Assignable(s.op, out) {
		return 0, errTypeError
	}
	n := frame.Copy(out, s.frame)
	m := s.frame.Len()
	s.frame = s.frame.Slice(n, m)
	if m == 0 {
		return n, sliceio.EOF
	}
	return n, nil
}

func (s *constSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	n := s.frame.Len()
	if n == 0 {
		return sliceio.EmptyReader{}
	}
	// The last shard gets truncated when the data cannot be split
	// evenly.
	shardn := (n / s.nshard) + 1
	beg := shardn * shard
	end := beg + shardn
	if beg >= n {
		return sliceio.EmptyReader{}
	}
	if end >= n {
		end = n
	}
	r := &constReader{
		op:    s,
		frame: s.frame.Slice(beg, end),
		shard: shard,
	}
	return r
}

type readerFuncSlice struct {
	slicetype.Type
	nshard    int
	read      reflect.Value
	stateType reflect.Type
}

// ReaderFunc returns a Slice that uses the provided function to read
// data. The function read must be of the form:
//
//	func(shard int, state stateType, col1 []col1Type, col2 []col2Type, ..., colN []colNType) (int, error)
//
// This returns a slice of the form:
//
//	Slice<col1Type, col2Type, ..., colNType>
//
// The function is invoked to fill a vector of elements. col1, ...,
// colN are preallocated slices that should be filled by the reader
// function. The function should return the number of elements that
// were filled. The error EOF should be returned when no more data
// are available.
//
// ReaderFunc provides the function with a zero-value state upon the
// first invocation of the function for a given shard. (If the state
// argument is a pointer, it is allocated.) Subsequent invocations of
// the function receive the same state value, thus permitting the
// reader to maintain local state across the read of a whole shard.
func ReaderFunc(nshard int, read interface{}) Slice {
	s := new(readerFuncSlice)
	s.nshard = nshard
	s.read = reflect.ValueOf(read)
	arg, ret, ok := typecheck.Func(read)
	if !ok || arg.NumOut() < 3 || arg.Out(0).Kind() != reflect.Int {
		typecheck.Panicf(1, "readerfunc: invalid reader function type %T", read)
	}
	if ret.Out(0).Kind() != reflect.Int || ret.Out(1) != typeOfError {
		typecheck.Panicf(1, "readerfunc: function %T does not return (int, error)", read)
	}
	s.stateType = arg.Out(1)
	arg = slicetype.Slice(arg, 2, arg.NumOut())
	if s.Type, ok = typecheck.Devectorize(arg); !ok {
		typecheck.Panicf(1, "readerfunc: function %T is not vectorized", read)
	}
	return s
}

func (*readerFuncSlice) Op() string               { return "reader" }
func (r *readerFuncSlice) NumShard() int          { return r.nshard }
func (*readerFuncSlice) ShardType() ShardType     { return HashShard }
func (*readerFuncSlice) NumDep() int              { return 0 }
func (*readerFuncSlice) Dep(i int) Dep            { panic("no deps") }
func (*readerFuncSlice) Combiner() *reflect.Value { return nil }

type readerFuncSliceReader struct {
	op    *readerFuncSlice
	state reflect.Value
	shard int
	err   error
}

func (r *readerFuncSliceReader) Read(ctx context.Context, out frame.Frame) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if !slicetype.Assignable(out, r.op) {
		return 0, errTypeError
	}
	// Initialize state (on first call)
	if !r.state.IsValid() {
		if r.op.stateType.Kind() == reflect.Ptr {
			r.state = reflect.New(r.op.stateType.Elem())
		} else {
			r.state = reflect.Zero(r.op.stateType)
		}
	}
	rvs := r.op.read.Call(append([]reflect.Value{reflect.ValueOf(r.shard), r.state}, out.Values()...))
	n = int(rvs[0].Int())
	if e := rvs[1].Interface(); e != nil {
		if err := e.(error); err == sliceio.EOF || errors.Recover(err).Severity != errors.Unknown {
			r.err = err
		} else {
			// We consider all application-generated errors as Fatal unless marked otherwise.
			r.err = errors.E(errors.Fatal, err)
		}
	}
	return n, r.err
}

func (r *readerFuncSlice) Reader(shard int, reader []sliceio.Reader) sliceio.Reader {
	return &readerFuncSliceReader{op: r, shard: shard}
}

type mapSlice struct {
	Slice
	fval reflect.Value
	out  slicetype.Type
}

// Map transforms a slice by invoking a function for each record. The
// type of slice must match the arguments of the function fn. The
// type of the returned slice is the set of columns returned by fn.
// The returned slice matches the input slice's sharding, but is always
// hash partitioned.
//
// Schematically:
//
//	Map(Slice<t1, t2, ..., tn>, func(v1 t1, v2 t2, ..., vn tn) (r1, r2, ..., rn)) Slice<r1, r2, ..., rn>
func Map(slice Slice, fn interface{}) Slice {
	m := new(mapSlice)
	m.Slice = slice
	m.fval = reflect.ValueOf(fn)
	arg, ret, ok := typecheck.Func(fn)
	if !ok {
		typecheck.Panicf(1, "map: invalid map function %T", fn)
	}
	if !typecheck.Equal(slice, arg) {
		typecheck.Panicf(1, "map: function %T does not match input slice type %s", fn, slicetype.String(slice))
	}
	if ret.NumOut() == 0 {
		typecheck.Panicf(1, "map: need at least one output column")
	}
	m.out = ret
	return m
}

func (m *mapSlice) NumOut() int            { return m.out.NumOut() }
func (m *mapSlice) Out(c int) reflect.Type { return m.out.Out(c) }
func (*mapSlice) ShardType() ShardType     { return HashShard }
func (m *mapSlice) Op() string             { return "map" }
func (*mapSlice) NumDep() int              { return 1 }
func (m *mapSlice) Dep(i int) Dep          { return singleDep(i, m.Slice, false) }
func (*mapSlice) Combiner() *reflect.Value { return nil }

type mapReader struct {
	op     *mapSlice
	reader sliceio.Reader // parent reader
	in     frame.Frame    // buffer for input column vectors
	err    error
}

func (m *mapReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	if !slicetype.Assignable(out, m.op) {
		return 0, errTypeError
	}
	n := out.Len()
	if !m.in.IsValid() {
		m.in = frame.Make(m.op.Slice, n, n)
	} else {
		m.in = m.in.Ensure(n)
	}
	n, m.err = m.reader.Read(ctx, m.in.Slice(0, n))
	// Now iterate over each record, transform it, and set the output
	// records. Note that we could parallelize the map operation here,
	// but for simplicity, parallelism should be achieved by finer
	// sharding instead, simplifying management of parallel
	// computation.
	//
	// TODO(marius): provide a vectorized version of map for efficiency.
	args := make([]reflect.Value, m.in.NumOut())
	for i := 0; i < n; i++ {
		// Gather the arguments for a single invocation.
		for j := range args {
			args[j] = m.in.Index(j, i)
		}
		// TODO(marius): consider using an unsafe copy here
		result := m.op.fval.Call(args)
		for j := range result {
			out.Index(j, i).Set(result[j])
		}
	}
	return n, m.err
}

func (m *mapSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &mapReader{op: m, reader: deps[0]}
}

type filterSlice struct {
	Slice
	pred reflect.Value
}

// Filter returns a slice where the provided predicate is applied to
// each element in the given slice. The output slice contains only
// those entries for which the predicate is true.
//
// The predicate function should receive each column of slice
// and return a single boolean value.
//
// Schematically:
//
//	Filter(Slice<t1, t2, ..., tn>, func(t1, t2, ..., tn) bool) Slice<t1, t2, ..., tn>
func Filter(slice Slice, pred interface{}) Slice {
	f := new(filterSlice)
	f.Slice = slice
	f.pred = reflect.ValueOf(pred)
	arg, ret, ok := typecheck.Func(pred)
	if !ok {
		typecheck.Panicf(1, "filter: invalid predicate function %T", pred)
	}
	if !typecheck.Equal(slice, arg) {
		typecheck.Panicf(1, "filter: function %T does not match input slice type %s", pred, slicetype.String(slice))
	}
	if ret.NumOut() != 1 || ret.Out(0).Kind() != reflect.Bool {
		typecheck.Panic(1, "filter: predicate must return a single boolean value")
	}
	return f
}

func (*filterSlice) Op() string               { return "filter" }
func (*filterSlice) NumDep() int              { return 1 }
func (f *filterSlice) Dep(i int) Dep          { return singleDep(i, f.Slice, false) }
func (*filterSlice) Combiner() *reflect.Value { return nil }

type filterReader struct {
	op     *filterSlice
	reader sliceio.Reader
	in     frame.Frame
	err    error
}

func (f *filterReader) Read(ctx context.Context, out frame.Frame) (n int, err error) {
	if f.err != nil {
		return 0, f.err
	}
	if !slicetype.Assignable(out, f.op) {
		return 0, errTypeError
	}
	var (
		m   int
		max = out.Len()
	)
	args := make([]reflect.Value, out.NumOut())
	for m < max && f.err == nil {
		// TODO(marius): this can get pretty inefficient when the accept
		// rate is low: as we fill the output; we could degenerate into a
		// case where we issue a call for each element. Consider input
		// buffering instead.
		if !f.in.IsValid() {
			f.in = frame.Make(f.op, max-m, max-m)
		} else {
			f.in = f.in.Ensure(max - m)
		}
		n, f.err = f.reader.Read(ctx, f.in)
		for i := 0; i < n; i++ {
			for j := range args {
				args[j] = f.in.Value(j).Index(i)
			}
			if f.op.pred.Call(args)[0].Bool() {
				frame.Copy(out.Slice(m, m+1), f.in.Slice(i, i+1))
				m++
			}
		}
	}
	return m, f.err
}

func (f *filterSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &filterReader{op: f, reader: deps[0]}
}

type flatmapSlice struct {
	Slice
	file string
	line int
	fval reflect.Value
	out  slicetype.Type
}

// Flatmap returns a Slice that applies the function fn to each
// record in the slice, flattening the returned slice. That is, the
// function fn should be of the form:
//
//	func(in1 inType1, in2 inType2, ...) (out1 []outType1, out2 []outType2)
//
// Schematically:
//
//	Flatmap(Slice<t1, t2, ..., tn>, func(v1 t1, v2 t2, ..., vn tn) ([]r1, []r2, ..., []rn)) Slice<r1, r2, ..., rn>
func Flatmap(slice Slice, fn interface{}) Slice {
	f := new(flatmapSlice)
	f.Slice = slice
	f.fval = reflect.ValueOf(fn)
	arg, ret, ok := typecheck.Func(fn)
	if !ok {
		typecheck.Panicf(1, "flatmap: invalid flatmap function %T", fn)
	}
	if !typecheck.Equal(slice, arg) {
		typecheck.Panicf(1, "flatmap: flatmap function %T does not match input slice type %s", fn, slicetype.String(slice))
	}
	f.out, ok = typecheck.Devectorize(ret)
	if !ok {
		typecheck.Panicf(1, "flatmap: flatmap function %T is not vectorized", fn)
	}
	_, f.file, f.line, ok = runtime.Caller(1)
	if !ok {
		log.Print("bigslice.Flatmap: failed to retrieve caller location")
	}
	return f
}

func (f *flatmapSlice) NumOut() int            { return f.out.NumOut() }
func (f *flatmapSlice) Out(c int) reflect.Type { return f.out.Out(c) }
func (*flatmapSlice) ShardType() ShardType     { return HashShard }
func (*flatmapSlice) Op() string               { return "flatmap" }
func (*flatmapSlice) NumDep() int              { return 1 }
func (f *flatmapSlice) Dep(i int) Dep          { return singleDep(i, f.Slice, false) }
func (*flatmapSlice) Combiner() *reflect.Value { return nil }

type flatmapReader struct {
	op     *flatmapSlice
	reader sliceio.Reader // underlying reader

	in           frame.Frame // buffer of inputs
	begIn, endIn int
	out          frame.Frame // buffer of outputs
	eof          bool
}

func (f *flatmapReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	if !slicetype.Assignable(out, f.op) {
		return 0, errTypeError
	}
	args := make([]reflect.Value, f.op.Slice.NumOut())
	begOut, endOut := 0, out.Len()
	// Add buffered output from last call, if any.
	if f.out.Len() > 0 {
		n := frame.Copy(out, f.out)
		begOut += n
		f.out = f.out.Slice(n, f.out.Len())
	}
	// Continue as long as we have (possibly buffered) input, and space
	// for output.
	for begOut < endOut && (!f.eof || f.begIn < f.endIn) {
		if f.begIn == f.endIn {
			// out[0].Len() may not be related to an actually useful size, but we'll go with it.
			// TODO(marius): maybe always default to a fixed chunk size? Or
			// dynamically keep track of the average input:output ratio?
			if !f.in.IsValid() {
				f.in = frame.Make(f.op.Slice, out.Len(), out.Len())
			} else {
				f.in = f.in.Ensure(out.Len())
			}
			n, err := f.reader.Read(ctx, f.in)
			if err != nil && err != sliceio.EOF {
				return 0, err
			}
			f.begIn, f.endIn = 0, n
			f.eof = err == sliceio.EOF
		}
		// Consume one input at a time, as long as we have space in our
		// output buffer.
		for ; f.begIn < f.endIn && begOut < endOut; f.begIn++ {
			for j := range args {
				args[j] = f.in.Index(j, f.begIn)
			}
			result := frame.Values(f.op.fval.Call(args))
			n := frame.Copy(out.Slice(begOut, endOut), result)
			begOut += n
			// We've run out of output space. In this case, stash the rest of
			// our output into f.out, if any.
			if m := result.Len(); n < m {
				f.out = result.Slice(n, m)
			}
		}
	}
	var err error
	// We're EOF if we've encountered an EOF from the underlying
	// reader, there's no buffered output, and no buffered input.
	if f.eof && !f.out.IsValid() && f.begIn == f.endIn {
		err = sliceio.EOF
	}
	return begOut, err
}

func (f *flatmapSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &flatmapReader{op: f, reader: deps[0]}
}

type foldSlice struct {
	Slice
	fval reflect.Value
	out  slicetype.Type
	dep  Dep
}

// Fold returns a slice that aggregates values by the first column
// using a custom aggregation function. For an input slice
// Slice<t1, t2, ..., tn>, Fold requires that the provided accumulator
// function follow the form:
//
//	func(accum acctype, v2 t2, ..., vn tn) acctype
//
// The function is invoked once for each slice element with the same
// value for column 1 (t1). On the first invocation, the accumulator
// is passed the zero value of its accumulator type.
//
// Fold requires that the first column of the slice is partitionable.
// See the documentation for Keyer for more details.
//
// Schematically:
//
//	Fold(Slice<t1, t2, ..., tn>, func(accum acctype, v2 t2, ..., vn tn) acctype) Slice<t1, acctype>
func Fold(slice Slice, fold interface{}) Slice {
	if n := slice.NumOut(); n < 2 {
		typecheck.Panicf(1, "Fold can be applied only for slices with at least two columns; got %d", n)
	}
	if !frame.CanHash(slice.Out(0)) {
		typecheck.Panicf(1, "fold: key type %s is not partitionable", slice.Out(0))
	}
	if !canMakeAccumulatorForKey(slice.Out(0)) {
		typecheck.Panicf(1, "fold: key type %s cannot be accumulated", slice.Out(0))
	}
	f := new(foldSlice)
	f.Slice = slice
	// Fold requires shuffle by the first column.
	// TODO(marius): allow deps to express shuffling by other columns.
	f.dep = Dep{slice, true, false}
	f.fval = reflect.ValueOf(fold)

	arg, ret, ok := typecheck.Func(fold)
	if !ok {
		typecheck.Panicf(1, "fold: invalid fold function %T", fold)
	}
	if ret.NumOut() != 1 {
		typecheck.Panicf(1, "fold: fold functions must return exactly one value")
	}
	// func(acc, t2, t3, ..., tn)
	if got, want := arg, slicetype.Append(ret, slicetype.Slice(slice, 1, slice.NumOut())); !typecheck.Equal(got, want) {
		typecheck.Panicf(1, "fold: expected func(acc, t2, t3, ..., tn), got %T", fold)
	}
	// output: key, accumulator
	f.out = slicetype.New(slice.Out(0), ret.Out(0))
	return f
}

func (f *foldSlice) NumOut() int            { return f.out.NumOut() }
func (f *foldSlice) Out(c int) reflect.Type { return f.out.Out(c) }
func (f *foldSlice) Op() string             { return "fold" }
func (*foldSlice) NumDep() int              { return 1 }
func (f *foldSlice) Dep(i int) Dep          { return f.dep }
func (*foldSlice) Combiner() *reflect.Value { return nil }

type foldReader struct {
	op     *foldSlice
	reader sliceio.Reader
	accum  Accumulator
	err    error
}

// Compute accumulates values across all keys in this shard. The entire
// output is buffered in memory.
func (f *foldReader) compute(ctx context.Context) (Accumulator, error) {
	in := frame.Make(f.op.dep, defaultChunksize, defaultChunksize)
	accum := makeAccumulator(f.op.dep.Out(0), f.op.out.Out(1), f.op.fval)
	for {
		n, err := f.reader.Read(ctx, in)
		if err != nil && err != sliceio.EOF {
			return nil, err
		}
		accum.Accumulate(in, n)
		if err == sliceio.EOF {
			return accum, nil
		}
	}
}

func (f *foldReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	if !slicetype.Assignable(out, f.op) {
		return 0, errTypeError
	}
	if f.accum == nil {
		f.accum, f.err = f.compute(ctx)
		if f.err != nil {
			return 0, f.err
		}
	}
	var n int
	n, f.err = f.accum.Read(out.Value(0), out.Value(1))
	return n, f.err
}

func (f *foldSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &foldReader{op: f, reader: deps[0]}
}

type headSlice struct {
	Slice
	n int
}

// Head returns a slice that returns at most the first n items from
// each shard of the underlying slice. Its type is the same as the
// provided slice.
func Head(slice Slice, n int) Slice {
	return headSlice{slice, n}
}

func (h headSlice) Op() string             { return fmt.Sprintf("head(%d)", h.n) }
func (headSlice) NumDep() int              { return 1 }
func (h headSlice) Dep(i int) Dep          { return singleDep(i, h.Slice, false) }
func (headSlice) Combiner() *reflect.Value { return nil }

type headReader struct {
	reader sliceio.Reader
	n      int
}

func (h headSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &headReader{deps[0], h.n}
}

func (h *headReader) Read(ctx context.Context, out frame.Frame) (n int, err error) {
	if h.n <= 0 {
		return 0, sliceio.EOF
	}
	n, err = h.reader.Read(ctx, out)
	h.n -= n
	if h.n < 0 {
		n -= -h.n
	}
	return
}

type scanSlice struct {
	Slice
	scan func(shard int, scanner *sliceio.Scanner) error
}

// Scan invokes a function for each shard of the input Slice.
// It returns a unit Slice: Scan is inteded to be used for its side
// effects.
func Scan(slice Slice, scan func(shard int, scanner *sliceio.Scanner) error) Slice {
	return scanSlice{slice, scan}
}

func (scanSlice) NumOut() int              { return 0 }
func (scanSlice) Out(c int) reflect.Type   { panic(c) }
func (scanSlice) Op() string               { return "scan" }
func (scanSlice) NumDep() int              { return 1 }
func (s scanSlice) Dep(i int) Dep          { return singleDep(i, s.Slice, false) }
func (scanSlice) Combiner() *reflect.Value { return nil }

type scanReader struct {
	slice  scanSlice
	shard  int
	reader sliceio.Reader
}

func (s *scanReader) Read(ctx context.Context, out frame.Frame) (n int, err error) {
	err = s.slice.scan(s.shard, &sliceio.Scanner{Type: s.slice.Slice, Reader: s.reader})
	if err == nil {
		err = sliceio.EOF
	}
	return 0, err
}

func (s scanSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &scanReader{s, shard, deps[0]}
}

// String returns a string describing the slice and its type.
func String(slice Slice) string {
	types := make([]string, slice.NumOut())
	for i := range types {
		types[i] = fmt.Sprint(slice.Out(i))
	}
	return fmt.Sprintf("%s<%s>", slice.Op(), strings.Join(types, ", "))
}

func singleDep(i int, slice Slice, shuffle bool) Dep {
	if i != 0 {
		panic(fmt.Sprintf("invalid dependency %d", i))
	}
	return Dep{slice, shuffle, false}
}
