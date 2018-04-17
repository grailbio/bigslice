// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package stats

import "testing"

func TestStats(t *testing.T) {
	coll := NewMap()
	var (
		x = coll.Int("x")
		_ = coll.Int("y")
	)
	if got, want := x.Get(), int64(0); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	x.Add(123)
	x.Add(123)
	if got, want := x.Get(), int64(123*2); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	all := make(Values)
	coll.AddAll(all)
	coll.AddAll(all)
	if got, want := len(all), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := all["x"], int64(123*4); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := all["y"], int64(0); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
