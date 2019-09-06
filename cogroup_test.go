// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice_test

import (
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
)

func TestCogroup(t *testing.T) {
	// This test relies on orderings of slices within rows to be
	// consistent.
	//
	// TODO(marius): extend assertEqual to account for this directly.
	exec.DoShuffleReaders = false
	defer func() {
		exec.DoShuffleReaders = true
	}()

	data1 := []interface{}{
		[]string{"z", "b", "d", "d"},
		[]int{1, 2, 3, 4},
	}
	data2 := []interface{}{
		[]string{"x", "y", "z", "d"},
		[]string{"one", "two", "three", "four"},
	}
	sharding := [][]int{{1, 1}, {1, 4}, {2, 1}, {4, 4}}
	for _, shard := range sharding {
		slice1 := bigslice.Const(shard[0], data1...)
		slice2 := bigslice.Const(shard[1], data2...)

		assertEqual(t, bigslice.Cogroup(slice1, slice2), true,
			[]string{"b", "d", "x", "y", "z"},
			[][]int{{2}, {3, 4}, nil, nil, {1}},
			[][]string{nil, {"four"}, {"one"}, {"two"}, {"three"}},
		)
		assertEqual(t, bigslice.Cogroup(slice2, slice1), true,
			[]string{"b", "d", "x", "y", "z"},
			[][]string{nil, {"four"}, {"one"}, {"two"}, {"three"}},
			[][]int{{2}, {3, 4}, nil, nil, {1}},
		)
		// Should work equally well for one slice.
		assertEqual(t, bigslice.Cogroup(slice1), true,
			[]string{"b", "d", "z"},
			[][]int{{2}, {3, 4}, {1}},
		)
		if testing.Short() {
			break
		}
	}
}
