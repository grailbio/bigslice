// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package typecheck

import (
	"errors"
	"runtime"
	"testing"
)

func errorCaller(calldepth int, err error) (e *Error, file string, line int) {
	_, file, line, ok := runtime.Caller(calldepth + 1)
	if !ok {
		panic("not ok")
	}
	return NewError(calldepth+1, err), file, line
}

func TestError(t *testing.T) {
	e := errors.New("hello world")
	err, file, line := errorCaller(1, e)
	if got, want := err.Err, e; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := file, err.File; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := line, err.Line; got != want {
		t.Errorf("got %v, want %b", got, want)
	}
}
