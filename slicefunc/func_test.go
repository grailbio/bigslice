// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package slicefunc

import (
	"context"
	"reflect"
	"testing"
)

func TestFunc(t *testing.T) {
	ctx := context.Background()
	f, ok := Of(func(x, y int) int { return x + y })
	if !ok {
		t.Fatalf("unexpected bad func")
	}
	rv := f.Call(ctx, []reflect.Value{reflect.ValueOf(1), reflect.ValueOf(2)})
	if got, want := len(rv), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := rv[0].Int(), int64(3); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	f, ok = Of(func(pctx context.Context, x, y int) bool {
		return x+y == 3 && pctx == ctx
	})
	if !ok {
		t.Fatalf("unexpected bad func")
	}
	rv = f.Call(ctx, []reflect.Value{reflect.ValueOf(1), reflect.ValueOf(2)})
	if got, want := len(rv), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if !rv[0].Bool() {
		t.Error("!ok")
	}
}
