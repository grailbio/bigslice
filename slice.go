// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/grailbio/base/log"
)

// testCalldepth is used by tests to verify the correctness of
// caller attribution in error messages.
var testCalldepth = 0

var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

// DefaultChunkSize is the default size used for IO vectors throughout bigslice.
const defaultChunksize = 1024

// EOF is the error returned by (Reader).Read when no more data is
// available. EOF is intended as a sentinel error: it signals a
// graceful end of output. If output terminates unexpectedly, a
// different error should be returned.
var EOF = errors.New("EOF")

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

	// Hasher returns the hasher used to partition this slice's
	// inputs, if any.
	Hasher() FrameHasher

	// NumDep returns the number of dependencies of this Slice.
	NumDep() int
	// Dep returns the i'th dependency for this Slice.
	Dep(i int) Dep

	// Reader returns a Reader for a shard of this Slice. The reader
	// itself computes the shard's values on demand. The caller must
	// provide Readers for all of this shard's dependencies, constructed
	// according to the dependency type (see Dep).
	Reader(shard int, deps []Reader) Reader
}

type constSlice struct {
	columns Frame
	types   []reflect.Type
	nshard  int
}

// Const returns a Slice representing the provided value. Each column
// of the Slice should be provided as a Go slice of the column's
// type. The value is split into nshard shards.
func Const(nshard int, columns ...interface{}) Slice {
	if len(columns) == 0 {
		typePanicf(1, "const slice takes at least one column")
	}
	s := new(constSlice)
	s.nshard = nshard
	if s.nshard < 1 {
		typePanicf(1, "const slices need at least one shard")
	}
	s.columns = make(Frame, len(columns))
	s.types = make([]reflect.Type, len(columns))
	for i := range s.columns {
		s.columns[i] = reflect.ValueOf(columns[i])
		if s.columns[i].Kind() != reflect.Slice {
			typePanicf(1, "invalid column %d: expected slice, got %T", i, columns[i])
		}
		s.types[i] = s.columns[i].Type().Elem()
		if i > 0 && s.columns[i].Len() != s.columns[i-1].Len() {
			typePanicf(1, "column %d length does not match column %d", i, i-1)
		}
	}
	return s
}

func (s *constSlice) NumOut() int            { return len(s.columns) }
func (s *constSlice) Out(c int) reflect.Type { return s.types[c] }
func (*constSlice) Op() string               { return "const" }
func (s *constSlice) NumShard() int          { return s.nshard }
func (*constSlice) ShardType() ShardType     { return HashShard }
func (*constSlice) NumDep() int              { return 0 }
func (*constSlice) Dep(i int) Dep            { panic("no deps") }
func (*constSlice) Hasher() FrameHasher      { return nil }

type constReader struct {
	op      *constSlice
	columns Frame
	shard   int
}

func (s *constReader) Read(ctx context.Context, out Frame) (int, error) {
	if !Assignable(s.op, out) {
		return 0, errTypeError
	}
	n := CopyFrame(out, s.columns)
	m := s.columns.Len()
	s.columns = s.columns.Slice(n, m)
	if m == 0 {
		return n, EOF
	}
	return n, nil
}

func (s *constSlice) Reader(shard int, deps []Reader) Reader {
	n := s.columns[0].Len()
	if n == 0 {
		return emptyReader{}
	}
	// The last shard gets truncated when the data cannot be split
	// evenly.
	shardn := (n / s.nshard) + 1
	beg := shardn * shard
	end := beg + shardn
	if beg >= n {
		return emptyReader{}
	}
	if end >= n {
		end = n
	}
	r := &constReader{
		op:      s,
		columns: make(Frame, len(s.columns)),
		shard:   shard,
	}
	for i := range r.columns {
		r.columns[i] = s.columns[i].Slice(beg, end)
	}
	return r
}

type readerFuncSlice struct {
	nshard    int
	read      reflect.Value
	stateType reflect.Type
	out       []reflect.Type
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
	ftype := s.read.Type()
	if ftype.Kind() != reflect.Func {
		typePanicf(1, "expected a function, got %T", read)
	}
	if ftype.NumIn() < 3 {
		typePanicf(1, "reader functions need at least a shard argument, a state, and at least one column")
	}
	if ftype.In(0).Kind() != reflect.Int {
		typePanicf(1, "reader functions must have an integer shard number as its first argument")
	}
	s.stateType = ftype.In(1)
	s.out = make([]reflect.Type, ftype.NumIn()-2)
	for i := range s.out {
		argType := ftype.In(i + 2)
		if argType.Kind() != reflect.Slice {
			typePanicf(1, "wrong type for argument %d: expected a slice, got %s", i+2, argType)
		}
		s.out[i] = argType.Elem()
	}
	if ftype.NumOut() != 2 || ftype.Out(0).Kind() != reflect.Int || ftype.Out(1) != typeOfError {
		typePanicf(1, "reader function must return (int, error), got %s", ftype)
	}
	return s
}

func (r *readerFuncSlice) NumOut() int            { return len(r.out) }
func (r *readerFuncSlice) Out(c int) reflect.Type { return r.out[c] }
func (*readerFuncSlice) Op() string               { return "reader" }
func (r *readerFuncSlice) NumShard() int          { return r.nshard }
func (*readerFuncSlice) ShardType() ShardType     { return HashShard }
func (*readerFuncSlice) NumDep() int              { return 0 }
func (*readerFuncSlice) Dep(i int) Dep            { panic("no deps") }
func (*readerFuncSlice) Hasher() FrameHasher      { return nil }

type readerFuncSliceReader struct {
	op    *readerFuncSlice
	state reflect.Value
	shard int
	err   error
}

func (r *readerFuncSliceReader) Read(ctx context.Context, out Frame) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if !Assignable(out, r.op) {
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
	rvs := r.op.read.Call(append([]reflect.Value{reflect.ValueOf(r.shard), r.state}, out...))
	n = int(rvs[0].Int())
	if e := rvs[1].Interface(); e != nil {
		r.err = e.(error)
	}
	return n, r.err
}

func (r *readerFuncSlice) Reader(shard int, reader []Reader) Reader {
	return &readerFuncSliceReader{op: r, shard: shard}
}

type mapSlice struct {
	Slice
	fval reflect.Value
	out  []reflect.Type
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
	ftype := m.fval.Type()
	if ftype.Kind() != reflect.Func {
		typePanicf(1, "expected a function, got %T", fn)
	}
	if got, want := ftype.NumIn(), slice.NumOut(); got != want {
		if want == 1 {
			typePanicf(1, "expected 1 argument, got %d", got)
		}
		typePanicf(1, "expected %d arguments, got %d", want, got)
	}
	for i := 0; i < ftype.NumIn(); i++ {
		if got, want := ftype.In(i), slice.Out(i); got != want {
			typePanicf(1, "expected type %v for argument %d, got %v", want, i, got)
		}
	}
	if ftype.NumOut() == 0 {
		typePanicf(1, "map functions need at least one output column")
	}
	m.out = make([]reflect.Type, ftype.NumOut())
	for i := range m.out {
		m.out[i] = ftype.Out(i)
	}
	return m
}

func (m *mapSlice) NumOut() int            { return len(m.out) }
func (m *mapSlice) Out(c int) reflect.Type { return m.out[c] }
func (*mapSlice) ShardType() ShardType     { return HashShard }
func (m *mapSlice) Op() string             { return "map" }
func (*mapSlice) NumDep() int              { return 1 }
func (m *mapSlice) Dep(i int) Dep          { return singleDep(i, m.Slice, false) }
func (*mapSlice) Hasher() FrameHasher      { return nil }

type mapReader struct {
	op     *mapSlice
	reader Reader // parent reader
	in     Frame  // buffer for input column vectors
	err    error
}

func (m *mapReader) Read(ctx context.Context, out Frame) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	if !Assignable(out, m.op) {
		return 0, errTypeError
	}
	n := out.Len()
	m.in = m.in.Realloc(m.op.Slice, n)
	n, m.err = m.reader.Read(ctx, m.in)
	// Now iterate over each record, transform it, and set the output
	// records. Note that we could parallelize the map operation here,
	// but for simplicity, parallelism should be achieved by finer
	// sharding instead, simplifying management of parallel
	// computation.
	//
	// TODO(marius): provide a vectorized version of map for efficiency.
	args := make([]reflect.Value, len(m.in))
	for i := 0; i < n; i++ {
		// Gather the arguments for a single invocation.
		m.in.CopyIndex(args, i)
		out.SetIndex(m.op.fval.Call(args), i)
	}
	return n, m.err
}

func (m *mapSlice) Reader(shard int, deps []Reader) Reader {
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
	ftype := f.pred.Type()
	if ftype.Kind() != reflect.Func {
		typePanicf(1, "expected a function, got %T", pred)
	}
	if got, want := ftype.NumIn(), slice.NumOut(); got != want {
		typePanicf(1, "expected %d arguments, got %d", want, got)
	}
	if ftype.NumOut() != 1 || ftype.Out(0).Kind() != reflect.Bool {
		typePanicf(1, "predicates should return a single boolean value")
	}
	for i := 0; i < ftype.NumIn(); i++ {
		if got, want := ftype.In(i), slice.Out(i); got != want {
			typePanicf(1, "wrong type for argument %d: expected %s, not %s", i, want, got)
		}
	}
	return f
}

func (*filterSlice) Op() string      { return "filter" }
func (*filterSlice) NumDep() int     { return 1 }
func (f *filterSlice) Dep(i int) Dep { return singleDep(i, f.Slice, false) }

type filterReader struct {
	op     *filterSlice
	reader Reader
	in     Frame
	err    error
}

func (f *filterReader) Read(ctx context.Context, out Frame) (n int, err error) {
	if f.err != nil {
		return 0, f.err
	}
	if !Assignable(out, f.op) {
		return 0, errTypeError
	}
	var (
		m   int
		max = out.Len()
	)
	args := make([]reflect.Value, len(out))
	for m < max && f.err == nil {
		// TODO(marius): this can get pretty inefficient when the accept
		// rate is low: as we fill the output; we could degenerate into a
		// case where we issue a call for each element. Consider input
		// buffering instead.
		f.in = f.in.Realloc(f.op, max-m)
		n, f.err = f.reader.Read(ctx, f.in)
		for i := 0; i < n; i++ {
			f.in.CopyIndex(args, i)
			if f.op.pred.Call(args)[0].Bool() {
				for j := range out {
					out[j].Index(m).Set(f.in[j].Index(i))
				}
				m++
			}
		}
	}
	return m, f.err
}

func (f *filterSlice) Reader(shard int, deps []Reader) Reader {
	return &filterReader{op: f, reader: deps[0]}
}

type flatmapSlice struct {
	Slice
	file string
	line int
	fval reflect.Value
	out  []reflect.Type
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
	ftype := f.fval.Type()
	if ftype.Kind() != reflect.Func {
		typePanicf(1, "expected a function, got %T", fn)
	}
	if got, want := ftype.NumIn(), slice.NumOut(); got != want {
		typePanicf(1, "expected %d arguments, got %v", want, got)
	}
	for i := 0; i < slice.NumOut(); i++ {
		if got, want := ftype.In(i), slice.Out(i); got != want {
			typePanicf(1, "expected type %v for argument %d, got %v", want, i, got)
		}
	}
	if ftype.NumOut() == 0 {
		typePanicf(1, "flatmap functions must have at least one output parameter")
	}
	f.out = make([]reflect.Type, ftype.NumOut())
	for i := 0; i < ftype.NumOut(); i++ {
		if ftype.Out(i).Kind() != reflect.Slice {
			typePanicf(1, "output argument %d must be a slice, not %s", i, ftype.Out(i))
		}
		f.out[i] = ftype.Out(i).Elem()
	}
	var ok bool
	_, f.file, f.line, ok = runtime.Caller(1)
	if !ok {
		log.Print("bigslice.Flatmap: failed to retrieve caller location")
	}
	return f
}

func (f *flatmapSlice) NumOut() int            { return len(f.out) }
func (f *flatmapSlice) Out(c int) reflect.Type { return f.out[c] }
func (*flatmapSlice) ShardType() ShardType     { return HashShard }
func (*flatmapSlice) Op() string               { return "flatmap" }
func (*flatmapSlice) NumDep() int              { return 1 }
func (f *flatmapSlice) Dep(i int) Dep          { return singleDep(i, f.Slice, false) }
func (*flatmapSlice) Hasher() FrameHasher      { return nil }

type flatmapReader struct {
	op     *flatmapSlice
	reader Reader // underlying reader

	in           Frame // buffer of inputs
	begIn, endIn int
	out          Frame // buffer of outputs
	eof          bool
}

func (f *flatmapReader) Read(ctx context.Context, out Frame) (int, error) {
	if !Assignable(out, f.op) {
		return 0, errTypeError
	}
	args := make([]reflect.Value, f.op.Slice.NumOut())
	begOut, endOut := 0, out[0].Len()
	for i := 1; i < len(out); i++ {
		if out[i].Len() != endOut {
			return 0, fmt.Errorf("%s:%d: returned output vectors of different sizes", f.op.file, f.op.line)
		}
	}
	// Add buffered output from last call, if any.
	if f.out != nil {
		n := CopyFrame(out, f.out)
		begOut += n
		if n == f.out.Len() {
			f.out = nil
		} else {
			f.out = f.out.Slice(n, f.out.Len())
		}
	}
	// Continue as long as we have (possibly buffered) input, and space
	// for output.
	for begOut < endOut && (!f.eof || f.begIn < f.endIn) {
		if f.begIn == f.endIn {
			// out[0].Len() may not be related to an actually useful size, but we'll go with it.
			// TODO(marius): maybe always default to a fixed chunk size? Or
			// dynamically keep track of the average input:output ratio?
			f.in = f.in.Realloc(f.op.Slice, out.Len())
			n, err := f.reader.Read(ctx, f.in)
			if err != nil && err != EOF {
				return 0, err
			}
			f.begIn, f.endIn = 0, n
			f.eof = err == EOF
		}
		// Consume one input at a time, as long as we have space in our
		// output buffer.
		for ; f.begIn < f.endIn && begOut < endOut; f.begIn++ {
			f.in.CopyIndex(args, f.begIn)
			result := Frame(f.op.fval.Call(args))
			n := CopyFrame(out.Slice(begOut, endOut), result)
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
	if f.eof && f.out == nil && f.begIn == f.endIn {
		err = EOF
	}
	return begOut, err
}

func (f *flatmapSlice) Reader(shard int, deps []Reader) Reader {
	return &flatmapReader{op: f, reader: deps[0]}
}

type foldSlice struct {
	Slice
	hasher FrameHasher
	fval   reflect.Value
	ftype  reflect.Type
	out    []reflect.Type
	dep    Dep
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
		typePanicf(1, "Fold can be applied only for slices with at least two columns; got %d", n)
	}
	hasher := makeFrameHasher(slice.Out(0), 0)
	if hasher == nil {
		typePanicf(1, "key type %s is not partitionable", slice.Out(0))
	}
	if !canMakeAccumulatorForKey(slice.Out(0)) {
		typePanicf(1, "key type %s cannot be accumulated", slice.Out(0))
	}
	f := new(foldSlice)
	f.Slice = slice
	f.hasher = hasher
	// Fold requires shuffle by the first column.
	// TODO(marius): allow deps to express shuffling by other columns.
	f.dep = Dep{slice, true}
	f.fval = reflect.ValueOf(fold)
	f.ftype = f.fval.Type()
	if f.ftype.Kind() != reflect.Func {
		typePanicf(1, "expected a function, got %T", fold)
	}
	// Accumulator plus remainder of columns
	if got, want := f.ftype.NumIn(), slice.NumOut(); got != want {
		typePanicf(1, "expected %d arguments, got %d", want, got)
	}
	if n := f.ftype.NumOut(); n != 1 {
		typePanicf(1, "accumulators must return a single value, not %d", n)
	}
	if got, want := f.ftype.Out(0), f.ftype.In(0); got != want {
		typePanicf(1, "expected output type %s, got %s", want, got)
	}
	for i := 1; i < f.ftype.NumIn(); i++ {
		if got, want := f.ftype.In(i), slice.Out(i); got != want {
			typePanicf(1, "wrong type for argument %d: expected %s, not %s", i, want, got)
		}
	}
	// Output: key, accumulated.
	f.out = []reflect.Type{slice.Out(0), f.ftype.Out(0)}
	return f
}

func (f *foldSlice) NumOut() int            { return len(f.out) }
func (f *foldSlice) Out(c int) reflect.Type { return f.out[c] }
func (f *foldSlice) Hasher() FrameHasher    { return f.hasher }
func (f *foldSlice) Op() string             { return "fold" }
func (*foldSlice) NumDep() int              { return 1 }
func (f *foldSlice) Dep(i int) Dep          { return f.dep }

type foldReader struct {
	op     *foldSlice
	reader Reader
	accum  Accumulator
	err    error
}

// Compute accumulates values across all keys in this shard. The entire
// output is buffered in memory.
func (f *foldReader) compute(ctx context.Context) (Accumulator, error) {
	in := MakeFrame(f.op.dep, defaultChunksize)
	accum := makeAccumulator(f.op.dep.Out(0), f.op.out[1], f.op.fval)
	for {
		n, err := f.reader.Read(ctx, in)
		if err != nil && err != EOF {
			return nil, err
		}
		accum.Accumulate(in, n)
		if err == EOF {
			return accum, nil
		}
	}
}

func (f *foldReader) Read(ctx context.Context, out Frame) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	if !Assignable(out, f.op) {
		return 0, errTypeError
	}
	if f.accum == nil {
		f.accum, f.err = f.compute(ctx)
		if f.err != nil {
			return 0, f.err
		}
	}
	var n int
	n, f.err = f.accum.Read(out[0], out[1])
	return n, f.err
}

func (f *foldSlice) Reader(shard int, deps []Reader) Reader {
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

func (h headSlice) Op() string    { return fmt.Sprintf("head(%d)", h.n) }
func (headSlice) NumDep() int     { return 1 }
func (h headSlice) Dep(i int) Dep { return singleDep(i, h.Slice, false) }

type headReader struct {
	reader Reader
	n      int
}

func (h headSlice) Reader(shard int, deps []Reader) Reader {
	return &headReader{deps[0], h.n}
}

func (h *headReader) Read(ctx context.Context, out Frame) (n int, err error) {
	if h.n <= 0 {
		return 0, EOF
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
	scan func(shard int, scanner *Scanner) error
}

// Scan invokes a function for each shard of the input Slice.
// It returns a unit Slice: Scan is inteded to be used for its side
// effects.
func Scan(slice Slice, scan func(shard int, scanner *Scanner) error) Slice {
	return scanSlice{slice, scan}
}

func (scanSlice) NumOut() int            { return 0 }
func (scanSlice) Out(c int) reflect.Type { panic(c) }
func (scanSlice) Op() string             { return "scan" }
func (scanSlice) NumDep() int            { return 1 }
func (s scanSlice) Dep(i int) Dep        { return singleDep(i, s.Slice, false) }

type scanReader struct {
	slice  scanSlice
	shard  int
	reader Reader
}

func (s *scanReader) Read(ctx context.Context, out Frame) (n int, err error) {
	err = s.slice.scan(s.shard, &Scanner{out: ColumnTypes(s.slice.Slice), readers: []Reader{s.reader}})
	if err == nil {
		err = EOF
	}
	return 0, err
}

func (s scanSlice) Reader(shard int, deps []Reader) Reader {
	return &scanReader{s, shard, deps[0]}
}

// ColumnTypes returns all column types of the provided slice
// in a single slice.
func ColumnTypes(slice Slice) []reflect.Type {
	out := make([]reflect.Type, slice.NumOut())
	for i := range out {
		out[i] = slice.Out(i)
	}
	return out
}

// String returns a string describing the slice and its type.
func String(slice Slice) string {
	types := make([]string, slice.NumOut())
	for i := range types {
		types[i] = fmt.Sprint(slice.Out(i))
	}
	return fmt.Sprintf("%s<%s>", slice.Op(), strings.Join(types, ", "))
}

type typeError struct {
	err  error
	file string
	line int
}

func newTypeError(calldepth int, err error) typeError {
	e := typeError{err: err}
	var ok bool
	_, e.file, e.line, ok = runtime.Caller(calldepth + 1 + testCalldepth)
	if !ok {
		e.file = "<unknown>"
	}
	return e
}

func typeErrorf(calldepth int, format string, args ...interface{}) typeError {
	return newTypeError(calldepth+1, fmt.Errorf(format, args...))
}

func typePanicf(calldepth int, format string, args ...interface{}) {
	panic(typeErrorf(calldepth+1, format, args...))
}

func (err typeError) Error() string {
	return fmt.Sprintf("%s:%d: %v", err.file, err.line, err.err)
}

func singleDep(i int, slice Slice, shuffle bool) Dep {
	if i != 0 {
		panic(fmt.Sprintf("invalid dependency %d", i))
	}
	return Dep{slice, shuffle}
}
