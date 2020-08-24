// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package example

import (
	"github.com/grailbio/bigslice"
)

// IntMax computes the maximum integer (by key) of slice, where slice has type
// Slice<K, int>. We will use this trivial slice to illustrate testing
// facilities. See max_test.go.
func IntMax(slice bigslice.Slice) bigslice.Slice {
	return bigslice.Reduce(slice, func(a, b int) int {
		if a < b {
			return b
		}
		return a
	})
}
