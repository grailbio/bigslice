// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"reflect"
	"sort"
	"testing"

	fuzz "github.com/google/gofuzz"
)

func TestTopN(t *testing.T) {
	const (
		min, max = 50, 100
		n        = 15
	)
	fz := fuzz.New()
	fz.NilChance(0)
	fz.NumElements(min, max)
	var counts []int
	fz.Fuzz(&counts)
	sorted := make([]int, len(counts))
	copy(sorted, counts)
	sort.Ints(sorted)
	sorted = sorted[len(sorted)-n:]
	topIndices := topn(counts, n)
	top := make([]int, n)
	for i, j := range topIndices {
		top[i] = counts[j]
	}
	sort.Ints(top)
	if got, want := top, sorted; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
