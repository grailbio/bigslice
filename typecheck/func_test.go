// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package typecheck

import (
	"context"
	"testing"

	"github.com/grailbio/bigslice/slicetype"
)

func TestFunc(t *testing.T) {
	arg, ret, ok := Func(func(int, string) string { return "" })
	if !ok {
		t.Fatal("!ok")
	}
	if got, want := arg, slicetype.New(typeOfInt, typeOfString); !Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := ret, slicetype.New(typeOfString); !Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFuncContext(t *testing.T) {
	arg, ret, ok := Func(func(ctx context.Context, x int) bool { return false })
	if !ok {
		t.Fatal("!ok")
	}
	if got, want := arg, slicetype.New(typeOfInt); !Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := ret, slicetype.New(typeOfBool); !Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
