// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics_test

import (
	"testing"

	"github.com/grailbio/bigslice/metrics"
)

func TestCounter(t *testing.T) {
	var (
		a, b metrics.Scope
		c    = metrics.NewCounter()
	)
	c.Incr(&a, 2)
	if got, want := c.Value(&a), int64(2); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	c.Incr(&b, 123)
	if got, want := c.Value(&a), int64(2); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := c.Value(&b), int64(123); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	a.Merge(&b)
	if got, want := c.Value(&a), int64(125); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
