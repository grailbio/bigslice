// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"
	"testing"

	"github.com/grailbio/bigslice/frame"
)

func TestStringIndexer(t *testing.T) {
	strs := []string{"x", "x", "y", "z", "x"}
	f := frame.Columns(strs)
	ix := make(stringIndexer)
	indices := make([]int, len(strs))
	ix.Index(f, indices)
	if got, want := indices, ([]int{0, 0, 1, 2, 0}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
