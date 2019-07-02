// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package typecheck

import (
	"errors"
	"fmt"
	"runtime"
)

// TestCalldepth may be overriden by a user to add call depths to
// errors that are constructed by NewError. This is useful for
// testing that error messages capture the correct locations.
var TestCalldepth = 0

// Error represents a typechecking error. It wraps an underlying
// error with a location, as captured by NewError.
type Error struct {
	Err  error
	File string
	Line int
}

// NewError creates a new typechecking error at the given calldepth.
// The returned Error wraps err with the caller's location.
func NewError(calldepth int, err error) *Error {
	e := &Error{Err: err}
	var ok bool
	_, e.File, e.Line, ok = runtime.Caller(calldepth + 1 + TestCalldepth)
	if !ok {
		e.File = "<unknown>"
	}
	return e
}

// Errorf constructs an error in the manner of fmt.Errorf.
func Errorf(calldepth int, format string, args ...interface{}) *Error {
	return NewError(calldepth+1, fmt.Errorf(format, args...))
}

// Panic constructs a typechecking error and then panics with it.
func Panic(calldepth int, message string) {
	panic(NewError(calldepth+1, errors.New(message)))
}

// Panicf constructs a new formatted typechecking error and then
// panics with it.
func Panicf(calldepth int, format string, args ...interface{}) {
	panic(Errorf(calldepth+1, format, args...))
}

// Error implements error.
func (err *Error) Error() string {
	return fmt.Sprintf("%s:%d: %v", err.File, err.Line, err.Err)
}

// Location rewrites typecheck errors to use the provided location
// instead of the one computed by Panic. This allows a caller to
// attribute typechecking errors where appropriate. Location should
// only be used as a defer function, as it recovers (and rewrites)
// panics.
//
//	file, line := ...
//	defer Location(file, line)
func Location(file string, line int) {
	e := recover()
	if e == nil {
		return
	}
	err, ok := e.(*Error)
	if !ok {
		panic(e)
	}
	err.File = file
	err.Line = line
	panic(err)
}
