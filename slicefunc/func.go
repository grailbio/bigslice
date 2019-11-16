// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package slicefunc provides types and code to call user-defined functions with
// Bigslice.
package slicefunc

import (
	"context"
	"reflect"

	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/typecheck"
)

// Nil is a nil Func.
var Nil Func

var typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()

// Func represents a user-defined function within Bigslice. Currently it's a
// simple shim that's used to determine whether a context should be supplied to
// the callee.
//
// TODO(marius): Evolve this abstraction over time to avoid the use of
// reflection. For example, we can generate (via a template) code for
// invocations on common types. Having this abstraction in place makes this
// possible to do without changing any of the callers.
//
// TODO(marius): Another possibility is to exploit the fact that we have already
// typechecked the data pipeline within Bigslice, and thus can avoid the
// per-call typechecking overhead that is incurred by reflection. For example,
// we could allow Funcs to mint a new re-usable call frame that we can write
// values directly into. This might get us most of the former but with more
// generality and arguably less complexity. We could even pre-generate generate
// call frames for each row in a frame, since that is re-used also.
//
// TODO(marius): consider using this package to allow user-defined funcs to
// return an error in the last argument.
type Func struct {
	// In and Out represent the slicetype of the function's input and output,
	// respectively.
	In, Out     slicetype.Type
	fn          reflect.Value
	contextFunc bool
}

// Of creates a Func from the provided function. Of panics if fn is not a
// func.
func Of(fn interface{}) Func {
	in, out, ok := typecheck.Func(fn)
	if !ok {
		panic("slicefunc.New: invalid func")
	}
	v := reflect.ValueOf(fn)
	t := v.Type()
	context := t.NumIn() > 0 && t.In(0) == typeOfContext
	return Func{in, out, v, context}
}

// Call invokes the function with the provided arguments, and returns the
// reflected return values.
//
// TODO(marius): using reflect.Value here is not ideal for performance,
// but there may be more holistic approaches (see above) since we're
// (almost) always invoking functions from frames.
func (f Func) Call(ctx context.Context, args []reflect.Value) []reflect.Value {
	if f.contextFunc {
		return f.fn.Call(append([]reflect.Value{reflect.ValueOf(ctx)}, args...))
	}
	return f.fn.Call(args)
}

// IsNil returns whether the Func f is nil.
func (f Func) IsNil() bool {
	return f.fn == reflect.Value{}
}
