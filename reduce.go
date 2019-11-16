// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"fmt"
	"strings"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/sortio"
	"github.com/grailbio/bigslice/typecheck"
)

// Reduce returns a slice that reduces elements pairwise. Reduce
// operations must be commutative and associative. Schematically:
//
//	Reduce(Slice<k, v>, func(v1, v2 v) v) Slice<k, v>
//
// The provided reducer function is invoked to aggregate values of
// type v. Reduce can perform map-side "combining", so that data are
// reduced to their aggregated value aggressively. This can often
// speed up computations significantly.
//
// The slice to be reduced must have exactly 1 residual column: that is,
// its prefix must leave just one column as the value column to be
// aggregated.
//
// TODO(marius): Reduce currently maintains the working set of keys
// in memory, and is thus appropriate only where the working set can
// fit in memory. For situations where this is not the case, Cogroup
// should be used instead (at an overhead). Reduce should spill to disk
// when necessary.
//
// TODO(marius): consider pushing combiners into task dependency
// definitions so that we can combine-read all partitions on one machine
// simultaneously.
func Reduce(slice Slice, reduce interface{}) Slice {
	if res := slice.NumOut() - slice.Prefix(); res != 1 {
		typecheck.Panicf(1, "the slice must only have one 1 residual column; has %d", res)
	}
	if err := canMakeCombiningFrame(slice); err != nil {
		typecheck.Panic(1, err.Error())
	}
	arg, ret, ok := typecheck.Func(reduce)
	if !ok {
		typecheck.Panicf(1, "reduce: invalid reduce function %T", reduce)
	}
	outputType := slice.Out(slice.NumOut() - 1)
	if arg.NumOut() != 2 || arg.Out(0) != outputType || arg.Out(1) != outputType || ret.NumOut() != 1 || ret.Out(0) != outputType {
		typecheck.Panicf(1, "reduce: invalid reduce function %T, expected func(%s, %s) %s", reduce, outputType, outputType, outputType)
	}
	return &reduceSlice{slice, makeName("reduce"), slicefunc.Of(reduce)}
}

// ReduceSlice implements "post shuffle" combining merge sort.
type reduceSlice struct {
	Slice
	name     Name
	combiner slicefunc.Func
}

func (r *reduceSlice) Name() Name               { return r.name }
func (*reduceSlice) NumDep() int                { return 1 }
func (r *reduceSlice) Dep(i int) Dep            { return Dep{r.Slice, true, true} }
func (r *reduceSlice) Combiner() slicefunc.Func { return r.combiner }

func (r *reduceSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	if len(deps) == 1 {
		return deps[0]
	}
	return sortio.Reduce(r, fmt.Sprintf("app-%d", shard), deps, r.combiner)
}

// CanMakeCombiningFrame tells whether the provided Frame type can be
// be made into a combining frame.
// Returns an error if types cannot be combined.
func canMakeCombiningFrame(typ slicetype.Type) error {
	var failingTypes []string
	for i := 0; i < typ.Prefix(); i++ {
		if !frame.CanHash(typ.Out(i)) || !frame.CanCompare(typ.Out(i)) {
			failingTypes = append(failingTypes, typ.Out(i).String())
		}
	}
	if len(failingTypes) == 0 {
		return nil
	}
	return fmt.Errorf("cannot combine values for keys of type: %s", strings.Join(failingTypes, ", "))
}
