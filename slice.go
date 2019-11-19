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
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/internal/defaultsize"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/typecheck"
)

var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

// DefaultChunkSize is the default size used for IO vectors throughout bigslice.
var defaultChunksize = defaultsize.Chunk

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
//
// Types that implement the Slice interface must be comparable.
type Slice interface {
	slicetype.Type

	// Name returns a unique (composite) name for this Slice that also has
	// useful context for diagnostic or status display.
	Name() Name

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
	// is performed if Nil.
	Combiner() slicefunc.Func

	// Reader returns a Reader for a shard of this Slice. The reader
	// itself computes the shard's values on demand. The caller must
	// provide Readers for all of this shard's dependencies, constructed
	// according to the dependency type (see Dep).
	Reader(shard int, deps []sliceio.Reader) sliceio.Reader
}

// Pragma comprises runtime directives used during bigslice
// execution.
type Pragma interface {
	// Exclusive indicates that a slice task should be given
	// exclusive access to the underlying machine.
	Exclusive() bool
	// Materialize indicates that the result of the slice task should be
	// materialized, i.e. break pipelining.
	Materialize() bool
}

// Pragmas composes multiple underlying Pragmas.
type Pragmas []Pragma

// Exclusive implements Pragma.
func (p Pragmas) Exclusive() bool {
	for _, q := range p {
		if q.Exclusive() {
			return true
		}
	}
	return false
}

// Materialize implements Pragma.
func (p Pragmas) Materialize() bool {
	for _, q := range p {
		if q.Materialize() {
			return true
		}
	}
	return false
}

type exclusive struct{}

func (exclusive) Exclusive() bool   { return true }
func (exclusive) Materialize() bool { return false }

// Exclusive is a Pragma that indicates the slice task
// should be given exclusive access to the machine
// that runs it.
var Exclusive Pragma = exclusive{}

type materialize struct{}

func (materialize) Exclusive() bool   { return false }
func (materialize) Materialize() bool { return true }

// ExperimentalMaterialize is a Pragma that indicates the slice task results
// should be materialized, i.e. not pipelined. You may want to use this to
// materialize and reuse results of tasks that would normally have been
// pipelined.
//
// It is tagged "experimental" because we are considering other ways of
// achieving this.
//
// TODO(jcharumilind): Consider doing this automatically for slices on which
// multiple slices depend.
var ExperimentalMaterialize Pragma = materialize{}

type constSlice struct {
	name Name
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
	s.name = makeName("const")
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

func (s *constSlice) Name() Name             { return s.name }
func (*constSlice) Prefix() int              { return 1 }
func (s *constSlice) NumShard() int          { return s.nshard }
func (*constSlice) ShardType() ShardType     { return HashShard }
func (*constSlice) NumDep() int              { return 0 }
func (*constSlice) Dep(i int) Dep            { panic("no deps") }
func (*constSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

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
	name Name
	Pragma
	slicetype.Type
	nshard    int
	read      slicefunc.Func
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
func ReaderFunc(nshard int, read interface{}, prags ...Pragma) Slice {
	s := new(readerFuncSlice)
	s.name = makeName("reader")
	s.nshard = nshard
	arg, ret, ok := typecheck.Func(read)
	if !ok || arg.NumOut() < 3 || arg.Out(0).Kind() != reflect.Int {
		typecheck.Panicf(1, "readerfunc: invalid reader function type %T", read)
	}
	s.read = slicefunc.Of(read)
	if ret.Out(0).Kind() != reflect.Int || ret.Out(1) != typeOfError {
		typecheck.Panicf(1, "readerfunc: function %T does not return (int, error)", read)
	}
	s.stateType = arg.Out(1)
	arg = slicetype.Slice(arg, 2, arg.NumOut())
	if s.Type, ok = typecheck.Devectorize(arg); !ok {
		typecheck.Panicf(1, "readerfunc: function %T is not vectorized", read)
	}
	s.Pragma = Pragmas(prags)
	return s
}

func (r *readerFuncSlice) Name() Name             { return r.name }
func (*readerFuncSlice) Prefix() int              { return 1 }
func (r *readerFuncSlice) NumShard() int          { return r.nshard }
func (*readerFuncSlice) ShardType() ShardType     { return HashShard }
func (*readerFuncSlice) NumDep() int              { return 0 }
func (*readerFuncSlice) Dep(i int) Dep            { panic("no deps") }
func (*readerFuncSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

type readerFuncSliceReader struct {
	op    *readerFuncSlice
	state reflect.Value
	shard int
	err   error

	// consecutiveEmptyCalls counts how many times op.read returned 0 elements consecutively.
	// Many empty calls may mean the user forgot to return sliceio.EOF, so we log a warning.
	consecutiveEmptyCalls int
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
	// out is passed to a user, zero it.
	out.Zero()
	rvs := r.op.read.Call(ctx, append([]reflect.Value{reflect.ValueOf(r.shard), r.state}, out.Values()...))
	n = int(rvs[0].Int())
	if n == 0 {
		r.consecutiveEmptyCalls++
		if r.consecutiveEmptyCalls > 7 && r.consecutiveEmptyCalls&(r.consecutiveEmptyCalls-1) == 0 {
			log.Printf("warning: reader func returned empty vector %d consecutive times; "+
				"don't forget sliceio.EOF", r.consecutiveEmptyCalls)
		}
	} else {
		r.consecutiveEmptyCalls = 0
	}
	if e := rvs[1].Interface(); e != nil {
		if err := e.(error); err == sliceio.EOF || errors.IsTemporary(err) {
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

type writerFuncSlice struct {
	name Name
	Slice
	stateType reflect.Type
	write     slicefunc.Func
}

// WriterFunc returns a Slice that is functionally equivalent to the input
// Slice, allowing for computation with side effects by the provided write
// function. The write function must be of the form:
//
//	func(shard int, state stateType, err error, col1 []col1Type, col2 []col2Type, ..., colN []colNType) error
//
// where the input slice is of the form:
//
//	Slice<col1Type, col2Type, ..., colNType>
//
// The write function is invoked with every read of the input Slice. Each
// column slice will be of the same length and will be populated with the data
// from the read. For performance, the passed column slices share memory with
// the internal frame of the read. Do not modify the data in them, and assume
// that they will be modified once write returns.
//
// Any error from the read, including EOF, will be passed as err to the write
// function. Note that err may be EOF when column lengths are >0, similar to
// the semantics of sliceio.Reader.Read.
//
// If the write function performs I/O, it is recommended that the I/O be
// buffered to allow downstream computations to progress.
//
// WriterFunc provides the function with a zero-value state upon the first
// invocation of the function for a given shard. (If the state argument is a
// pointer, it is allocated.) Subsequent invocations of the function receive
// the same state value, thus permitting the writer to maintain local state
// across the write of the whole shard.
func WriterFunc(slice Slice, write interface{}) Slice {
	s := new(writerFuncSlice)
	s.name = makeName("writer")
	s.Slice = slice

	// Our error messages for wrongly-typed write functions include a
	// description of the expected type, which we construct here.
	colTypElems := make([]string, slice.NumOut())
	for i := range colTypElems {
		colTypElems[i] = fmt.Sprintf("col%d %s", i+1, reflect.SliceOf(slice.Out(i)).String())
	}
	colTyps := strings.Join(colTypElems, ", ")
	expectTyp := fmt.Sprintf("func(shard int, state stateType, err error, %s) error", colTyps)

	die := func(msg string) {
		typecheck.Panicf(2, "writerfunc: invalid writer function type %T; %s", write, msg)
	}

	arg, ret, ok := typecheck.Func(write)
	if !ok ||
		arg.NumOut() != 3+slice.NumOut() ||
		arg.Out(0).Kind() != reflect.Int ||
		arg.Out(2) != typeOfError {
		die(fmt.Sprintf("must be %s", expectTyp))
	}
	s.stateType = arg.Out(1)
	for i := 0; i < slice.NumOut(); i++ {
		if reflect.SliceOf(slice.Out(i)) != arg.Out(i+3) {
			die(fmt.Sprintf("must be %s", expectTyp))
		}
	}
	if ret.NumOut() != 1 || ret.Out(0) != typeOfError {
		die("must return error")
	}
	s.write = slicefunc.Of(write)
	return s
}

func (s *writerFuncSlice) Name() Name             { return s.name }
func (*writerFuncSlice) NumDep() int              { return 1 }
func (s *writerFuncSlice) Dep(i int) Dep          { return singleDep(i, s.Slice, false) }
func (*writerFuncSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

type writerFuncReader struct {
	shard     int
	write     slicefunc.Func
	reader    sliceio.Reader
	stateType reflect.Type
	state     reflect.Value
	err       error
}

func (r *writerFuncReader) callWrite(ctx context.Context, err error, frame frame.Frame) error {
	args := []reflect.Value{reflect.ValueOf(r.shard), r.state}

	// TODO(jcharumilind): Cache error and column arguments, as they will
	// likely be the same from call to call.
	var errArg reflect.Value
	if err == nil {
		errArg = reflect.Zero(typeOfError)
	} else {
		errArg = reflect.ValueOf(err)
	}
	args = append(args, errArg)

	args = append(args, frame.Values()...)
	rvs := r.write.Call(ctx, args)
	if e := rvs[0].Interface(); e != nil {
		return e.(error)
	}
	return nil
}

func (r *writerFuncReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if !r.state.IsValid() {
		if r.stateType.Kind() == reflect.Ptr {
			r.state = reflect.New(r.stateType.Elem())
		} else {
			r.state = reflect.Zero(r.stateType)
		}
	}

	n, err := r.reader.Read(ctx, out)
	werr := r.callWrite(ctx, err, out.Slice(0, n))
	if werr != nil && (err == nil || err == sliceio.EOF) {
		if errors.IsTemporary(werr) {
			err = werr
		} else {
			err = errors.E(errors.Fatal, werr)
		}
	}
	r.err = err
	return n, err
}

func (s *writerFuncSlice) Reader(shard int, reader []sliceio.Reader) sliceio.Reader {
	return &writerFuncReader{
		shard:     shard,
		write:     s.write,
		reader:    reader[0],
		stateType: s.stateType,
	}
}

type mapSlice struct {
	name Name
	Pragma
	Slice
	fval slicefunc.Func
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
func Map(slice Slice, fn interface{}, prags ...Pragma) Slice {
	m := new(mapSlice)
	m.name = makeName("map")
	m.Slice = slice
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
	m.fval = slicefunc.Of(fn)
	m.out = ret
	m.Pragma = Pragmas(prags)
	return m
}

func (m *mapSlice) Name() Name             { return m.name }
func (m *mapSlice) NumOut() int            { return m.out.NumOut() }
func (m *mapSlice) Out(c int) reflect.Type { return m.out.Out(c) }
func (*mapSlice) ShardType() ShardType     { return HashShard }
func (*mapSlice) NumDep() int              { return 1 }
func (m *mapSlice) Dep(i int) Dep          { return singleDep(i, m.Slice, false) }
func (*mapSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

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
	if m.in.IsZero() {
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
		result := m.op.fval.Call(ctx, args)
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
	name Name
	Pragma
	Slice
	pred slicefunc.Func
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
func Filter(slice Slice, pred interface{}, prags ...Pragma) Slice {
	f := new(filterSlice)
	f.name = makeName("filter")
	f.Slice = slice
	f.Pragma = Pragmas(prags)
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
	f.pred = slicefunc.Of(pred)
	return f
}

func (f *filterSlice) Name() Name             { return f.name }
func (*filterSlice) NumDep() int              { return 1 }
func (f *filterSlice) Dep(i int) Dep          { return singleDep(i, f.Slice, false) }
func (*filterSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

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
		if f.in.IsZero() {
			f.in = frame.Make(f.op, max-m, max-m)
		} else {
			f.in = f.in.Ensure(max - m)
		}
		n, f.err = f.reader.Read(ctx, f.in)
		for i := 0; i < n; i++ {
			for j := range args {
				args[j] = f.in.Value(j).Index(i)
			}
			if f.op.pred.Call(ctx, args)[0].Bool() {
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
	name Name
	Pragma
	Slice
	fval slicefunc.Func
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
func Flatmap(slice Slice, fn interface{}, prags ...Pragma) Slice {
	f := new(flatmapSlice)
	f.name = makeName("flatmap")
	f.Slice = slice
	f.Pragma = Pragmas(prags)
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
	f.fval = slicefunc.Of(fn)
	return f
}

func (f *flatmapSlice) Name() Name             { return f.name }
func (f *flatmapSlice) NumOut() int            { return f.out.NumOut() }
func (f *flatmapSlice) Out(c int) reflect.Type { return f.out.Out(c) }
func (*flatmapSlice) ShardType() ShardType     { return HashShard }
func (*flatmapSlice) NumDep() int              { return 1 }
func (f *flatmapSlice) Dep(i int) Dep          { return singleDep(i, f.Slice, false) }
func (*flatmapSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

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
			if f.in.IsZero() {
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
			result := frame.Values(f.op.fval.Call(ctx, args))
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
	if f.eof && f.out.Len() == 0 && f.begIn == f.endIn {
		err = sliceio.EOF
	}
	return begOut, err
}

func (f *flatmapSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &flatmapReader{op: f, reader: deps[0]}
}

type foldSlice struct {
	name Name
	Slice
	fval slicefunc.Func
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
//
// BUG(marius): Fold does not yet support slice grouping
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
	f.name = makeName("fold")
	f.Slice = slice
	// Fold requires shuffle by the first column.
	// TODO(marius): allow deps to express shuffling by other columns.
	f.dep = Dep{slice, true, false}

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
	f.fval = slicefunc.Of(fold)
	// output: key, accumulator
	f.out = slicetype.New(slice.Out(0), ret.Out(0))
	return f
}

func (f *foldSlice) Name() Name             { return f.name }
func (f *foldSlice) NumOut() int            { return f.out.NumOut() }
func (f *foldSlice) Out(c int) reflect.Type { return f.out.Out(c) }
func (*foldSlice) NumDep() int              { return 1 }
func (f *foldSlice) Dep(i int) Dep          { return f.dep }
func (*foldSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

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
	name Name
	Slice
	n int
}

// Head returns a slice that returns at most the first n items from
// each shard of the underlying slice. Its type is the same as the
// provided slice.
func Head(slice Slice, n int) Slice {
	return &headSlice{makeName(fmt.Sprintf("head(%d)", n)), slice, n}
}

func (h *headSlice) Name() Name             { return h.name }
func (*headSlice) NumDep() int              { return 1 }
func (h *headSlice) Dep(i int) Dep          { return singleDep(i, h.Slice, false) }
func (*headSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

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
	name Name
	Slice
	scan func(shard int, scanner *sliceio.Scanner) error
}

// Scan invokes a function for each shard of the input Slice.
// It returns a unit Slice: Scan is inteded to be used for its side
// effects.
func Scan(slice Slice, scan func(shard int, scanner *sliceio.Scanner) error) Slice {
	return &scanSlice{makeName("scan"), slice, scan}
}

func (s *scanSlice) Name() Name             { return s.name }
func (*scanSlice) NumOut() int              { return 0 }
func (*scanSlice) Out(c int) reflect.Type   { panic(c) }
func (*scanSlice) NumDep() int              { return 1 }
func (s *scanSlice) Dep(i int) Dep          { return singleDep(i, s.Slice, false) }
func (*scanSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

type scanReader struct {
	slice  scanSlice
	shard  int
	reader sliceio.Reader
}

func (s *scanReader) Read(ctx context.Context, out frame.Frame) (n int, err error) {
	err = s.slice.scan(s.shard, sliceio.NewScanner(s.slice.Slice, sliceio.NopCloser(s.reader)))
	if err == nil {
		err = sliceio.EOF
	}
	return 0, err
}

func (s scanSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &scanReader{s, shard, deps[0]}
}

type prefixSlice struct {
	Slice
	prefix int
}

// Prefixed returns a slice with the provided prefix. A prefix determines
// the number of columns (starting at 0) in the slice that compose the
// key values for that slice for operations like reduce.
func Prefixed(slice Slice, prefix int) Slice {
	if prefix < 1 {
		typecheck.Panic(1, "prefixed: prefix must include at least one column")
	}
	if prefix > slice.NumOut() {
		typecheck.Panicf(1, "prefixed: prefix %d is greater than number of columns %d", prefix, slice.NumOut())
	}
	return &prefixSlice{slice, prefix}
}

func (p *prefixSlice) Prefix() int { return p.prefix }

// Unwrap returns the underlying slice if the provided slice is used
// only to amend the type of the slice it composes.
//
// TODO(marius): this is required to properly compile slices that use the
// prefix combinator; we should have a more general and robust solution
// to this.
func Unwrap(slice Slice) Slice {
	if slice, ok := slice.(*prefixSlice); ok {
		return Unwrap(slice.Slice)
	}
	return slice
}

// String returns a string describing the slice and its type.
func String(slice Slice) string {
	types := make([]string, slice.NumOut())
	for i := range types {
		types[i] = fmt.Sprint(slice.Out(i))
	}
	return fmt.Sprintf("%s<%s>", slice.Name().Op, strings.Join(types, ", "))
}

func singleDep(i int, slice Slice, shuffle bool) Dep {
	if i != 0 {
		panic(fmt.Sprintf("invalid dependency %d", i))
	}
	return Dep{slice, shuffle, false}
}

var (
	helperMu sync.Mutex
	helpers  = make(map[string]bool)
)

// Helper is used to mark a function as a helper function: names for
// newly created slices will be attributed to the caller of the
// function instead of the function itself.
func Helper() {
	helperMu.Lock()
	defer helperMu.Unlock()
	helpers[callerFunc(1)] = true
}

func callerFunc(skip int) string {
	var pc [2]uintptr
	n := runtime.Callers(skip+2, pc[:]) // skip + runtime.Callers + callerFunc
	if n == 0 {
		panic("bigslice: zero callers found")
	}
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()
	return frame.Function
}

// Name is a unique name for a slice, constructed with useful context for
// diagnostic or status display.
type Name struct {
	// Op is the operation that the slice performs (e.g. "reduce", "map")
	Op string
	// File is the file in which the slice was defined.
	File string
	// Line is the line in File at which the slice was defined.
	Line int
	// Index disambiguates slices created on the same File and Line.
	Index int
}

func (n Name) String() string {
	return fmt.Sprintf("%s@%s:%d", n.Op, n.File, n.Line)
}

func makeName(op string) Name {
	// Presume the correct frame is the caller of makeName,
	// but skip to the frame before the last helper, if any.
	var pc [50]uintptr             // consider at most 50 frames
	n := runtime.Callers(3, pc[:]) // caller of makeName, makeName, runtime.Callers.
	if n == 0 {
		panic("bigslice: no callers found")
	}
	frames := runtime.CallersFrames(pc[:n])
	helperMu.Lock()
	var found runtime.Frame
	for more := true; more; {
		var frame runtime.Frame
		frame, more = frames.Next()
		if found.PC == 0 {
			found = frame
		}
		if helpers[frame.Function] {
			found = runtime.Frame{}
		}
	}
	helperMu.Unlock()
	index := newNameIndex(op, found.File, found.Line)
	return Name{op, found.File, found.Line, index}
}

type sliceNameIndexerKey struct {
	op   string
	file string
	line int
}

var sliceNameIndexerMu sync.Mutex
var sliceNameIndexerMap = make(map[sliceNameIndexerKey]int)

func newNameIndex(op, file string, line int) int {
	key := sliceNameIndexerKey{op, file, line}
	sliceNameIndexerMu.Lock()
	defer sliceNameIndexerMu.Unlock()
	c := sliceNameIndexerMap[key]
	sliceNameIndexerMap[key]++
	return c
}
