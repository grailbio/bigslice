// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice_test

import (
	"fmt"
	"testing"

	"github.com/grailbio/bigslice"
)

func TestReshard(t *testing.T) {
	const N = 1000
	ints := make([]string, N)
	for i := range ints {
		ints[i] = fmt.Sprint(i)
	}
	sharding := []int{1, 2, 5, 10, 100}
	for _, source := range sharding {
		for _, dest := range sharding {
			slice := bigslice.Const(source, ints)
			slice = bigslice.Reshard(slice, dest)
			assertEqual(t, slice, true, ints)
		}
	}
}
