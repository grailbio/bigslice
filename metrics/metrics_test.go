// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics_test

import (
	"testing"

	"github.com/grailbio/bigslice/metrics"
)

func TestCounter(t *testing.T) {
	var scope metrics.Scope

	c := metrics.NewCounter()
	c.Incr(&scope, 2)

	if got, want := c.Value(&scope), uint64(2); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
