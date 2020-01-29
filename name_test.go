// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"runtime"
	"testing"
)

func sliceOperation(helper bool) Name {
	return MakeName("test") // line 16
}

func callSliceOp(helper bool) Name {
	if helper {
		Helper()
	}
	return sliceOperation(helper) // line 20
}

// Note: this test is quite brittle as it relies on actual line numbers.
func TestMakeName(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)

	if got, want := callSliceOp(false), (Name{"test", file, 20, 0}); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := callSliceOp(true), (Name{"test", file, 30, 0}); got != want { // line 30
		t.Errorf("got %v, want %v", got, want)
	}
}
