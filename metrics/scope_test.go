// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics_test

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
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
		a.Reset()
		a.Merge(&b)
		if got, want := c.Value(&a), test.incrB; got != want {
			t.Errorf("%v: got %v, want %v", test, got, want)
		}
		c.Incr(&a, test.incrA)
		if got, want := c.Value(&a), test.incrA+test.incrB; got != want {
			t.Errorf("%v: got %v, want %v", test, got, want)
		}
	}
}

func TestScopeGob(t *testing.T) {
	var (
		scope metrics.Scope
		c     = metrics.NewCounter()
		b     bytes.Buffer
	)
	c.Incr(&scope, 123)
	if err := gob.NewEncoder(&b).Encode(&scope); err != nil {
		t.Fatal(err)
	}
	scope.Reset()
	if got, want := c.Value(&scope), int64(0); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if err := gob.NewDecoder(&b).Decode(&scope); err != nil {
		t.Fatal(err)
	}
	if got, want := c.Value(&scope), int64(123); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestScopeGobEmpty(t *testing.T) {
	var scope metrics.Scope
	if err := gob.NewEncoder(ioutil.Discard).Encode(&scope); err != nil {
		t.Fatal(err)
	}
}
