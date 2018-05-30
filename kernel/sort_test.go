// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package kernel

import (
	"reflect"
	"testing"

	"github.com/grailbio/bigslice/frame"
)

type customType struct{ val int }

func TestLessSorter(t *testing.T) {
	sorter := LessSorter(func(vals []customType) func(i, j int) bool {
		return func(i, j int) bool {
			return vals[i].val < vals[j].val
		}
	})
	f := frame.Columns(
		[]customType{{5}, {1}, {4}},
		[]string{"five", "one", "four"},
	)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("frame not sorted")
	}
	if got, want := f[0].Interface().([]customType), ([]customType{{1}, {4}, {5}}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := f[1].Interface().([]string), ([]string{"one", "four", "five"}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
