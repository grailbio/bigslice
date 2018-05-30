// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package kernel

import "testing"

type concat interface {
	Concat(a, b string) string
}

type stringConcat struct{}

func (stringConcat) Concat(a, b string) string { return a + b }

func TestRegisterLookup(t *testing.T) {
	Register(typeOfString, stringConcat{})
	var concatter concat
	if !Lookup(typeOfString, &concatter) {
		t.Fatal("failed to look up concatter")
	}
	if Lookup(typeOfInt, &concatter) {
		t.Error("false concat lookup")
	}
	if got, want := concatter.Concat("x", "y"), "xy"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if _, ok := concatter.(stringConcat); !ok {
		t.Error("wrong type of concat")
	}
}
