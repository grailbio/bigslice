// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"fmt"
	"reflect"

	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/typecheck"
)

type reshardSlice struct {
	sliceOp
	Slice
}

// Reshard returns a slice that shuffles rows by prefix so that
// all rows with equal prefix values end up in the same shard.
// Rows are not sorted within a shard.
//
// The output slice has the same type as the input.
//
// TODO: Add ReshardSort, which also sorts keys within each shard.
func Reshard(slice Slice) Slice {
	if err := canMakeCombiningFrame(slice); err != nil {
		typecheck.Panic(1, err.Error())
	}
	return &reshardSlice{makeSliceOp("reshard"), slice}
}

func (r *reshardSlice) Op() (string, string, int) { return r.sliceOp.Op() }
func (*reshardSlice) NumDep() int                 { return 1 }
func (r *reshardSlice) Dep(i int) Dep             { return Dep{r.Slice, true, false} }
func (*reshardSlice) Combiner() *reflect.Value    { return nil }

func (r *reshardSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	if len(deps) != 1 {
		panic(fmt.Errorf("expected one dep, got %d", len(deps)))
	}
	return deps[0]
}
