// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics_test

import (
	"testing"

	"github.com/grailbio/bigslice/metrics"
)

func TestScopeMerge(t *testing.T) {
	c := metrics.NewCounter()

	for _, test := range []struct {
		incrA, incrB int64
	}{
		{0, 0},
		{0, 1},
		{2, 0},
		{100, 200},
	} {
		var a, b metrics.Scope
		c.Incr(&a, test.incrA)
		c.Incr(&b, test.incrB)
		a.Merge(&b)
		if got, want := c.Value(&a), test.incrA+test.incrB; got != want {
			t.Errorf("%v: got %v, want %v", test, got, want)
		}
	}
}
