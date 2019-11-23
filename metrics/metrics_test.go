// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics_test

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
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

func ExampleCounter() {
	filterCount := metrics.NewCounter()
	filterFunc := bigslice.Func(func() (slice bigslice.Slice) {
		slice = bigslice.Const(1, []int{1, 2, 3, 4, 5, 6})
		slice = bigslice.Filter(slice, func(ctx context.Context, i int) bool {
			scope := metrics.ContextScope(ctx)
			if i%2 == 0 {
				filterCount.Incr(scope, 1)
				return false
			}
			return true
		})
		return
	})

	sess := exec.Start(exec.Local)
	res, err := sess.Run(context.Background(), filterFunc)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("filtered:", filterCount.Value(res.Scope()))
	// Output: filtered: 3
}
