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
// generality and arguably less complexity. We could even pre-generate
// call frames for each row in a frame, since that is re-used also.
//
// TODO(marius): consider using this package to allow user-defined funcs to
// return an error in the last argument.
type Func struct {
	// In and Out represent the slicetype of the function's input and output,
	// respectively.
	In, Out slicetype.Type
	// IsVariadic is whether the function's final parameter is variadic. If it
	// is, In.Out(In.NumOut()-1) returns the parameter's implicit actual slice
	// type. For example, if this represents func(x int, y ...string):
	//
	//  fn.In.NumOut() == 2
	//  fn.In.Out(0) is the reflect.Type for "int"
	//  fn.In.Out(1) is the reflect.Type for "[]string"
	//  fn.IsVariadic == true
	IsVariadic  bool
	fn          reflect.Value
	contextFunc bool
}

type funcSliceType struct {
	reflect.Type
}

func (funcSliceType) Prefix() int { return 1 }

// Of creates a Func from the provided function, along with a bool indicating
// whether fn is a valid function. If it is not, the returned Func is invalid.
func Of(fn interface{}) (Func, bool) {
	t := reflect.TypeOf(fn)
	if t == nil {
		return Func{}, false
	}
	if t.Kind() != reflect.Func {
		return Func{}, false
	}
	in := make([]reflect.Type, t.NumIn())
	for i := range in {
		in[i] = t.In(i)
	}
	context := len(in) > 0 && in[0] == typeOfContext
	if context {
		in = in[1:]
	}
	return Func{
		In:          slicetype.New(in...),
		Out:         funcSliceType{t},
		IsVariadic:  t.IsVariadic(),
		fn:          reflect.ValueOf(fn),
		contextFunc: context,
	}, true
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
