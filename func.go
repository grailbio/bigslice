// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"encoding/gob"
	"reflect"
	"sync/atomic"

	"github.com/grailbio/bigslice/typecheck"
)

func init() {
	gob.Register([]interface{}{})
}

var typeOfSlice = reflect.TypeOf((*Slice)(nil)).Elem()

var (
	// Funcs is the global registry of funcs. We rely on deterministic
	// registration order. (This is guaranteed by Go's variable
	// initialization for a single compiler, which is sufficient for our
	// use.) It would definitely be nice to have a nicer way of doing
	// this (without the overhead of users minting their own names).
	funcs []*FuncValue
	// FuncsBusy is used to detect data races in registration.
	funcsBusy int32
)

// A FuncValue represents a Bigslice function, as returned by Func.
type FuncValue struct {
	fn    reflect.Value
	args  []reflect.Type
	index int
}

// NumIn returns the number of input arguments to f.
func (f *FuncValue) NumIn() int { return len(f.args) }

// In returns the i'th argument type of function f.
func (f *FuncValue) In(i int) reflect.Type { return f.args[i] }

// Invocation creates an invocation representing the function f
// applied to the provided arguments. Invocation panics with a type
// error if the provided arguments do not match in type or arity.
func (f *FuncValue) Invocation(args ...interface{}) Invocation {
	argTypes := make([]reflect.Type, len(args))
	for i, arg := range args {
		argTypes[i] = reflect.TypeOf(arg)
	}
	f.typecheck(argTypes...)
	return newInvocation(uint64(f.index), args...)
}

// Apply invokes the function f with the provided arguments,
// returning the computed Slice. Apply panics with a type error if
// argument type or arity do not match.
func (f *FuncValue) Apply(args ...interface{}) Slice {
	argv := make([]reflect.Value, len(args))
	for i := range argv {
		argv[i] = reflect.ValueOf(args[i])
	}
	return f.applyValue(argv)
}

func (f *FuncValue) applyValue(args []reflect.Value) Slice {
	argTypes := make([]reflect.Type, len(args))
	for i, arg := range args {
		argTypes[i] = arg.Type()
	}
	f.typecheck(argTypes...)
	out := f.fn.Call(args)
	return out[0].Interface().(Slice)
}

func (f *FuncValue) typecheck(args ...reflect.Type) {
	if len(args) != len(f.args) {
		typecheck.Panicf(2, "wrong number of arguments: function takes %d arguments, got %d",
			len(f.args), len(args))
	}
	for i := range args {
		expect, have := f.args[i], args[i]
		switch expect.Kind() {
		case reflect.Interface:
			if !have.Implements(expect) {
				typecheck.Panicf(2, "wrong type for argument %d: type %s does not implement interface %s", i, have, expect)
			}
		default:
			if have != expect {
				typecheck.Panicf(2, "wrong type for argument %d: expected %s, got %s", i, expect, have)
			}
		}
	}
}

// Func creates a bigslice function from the provided function value.
// Bigslice funcs must return a single Slice value. Funcs provide
// bigslice with a means of dynamic abstraction: since Funcs can be
// invoked remotely, dynamically created slices may be named across
// process boundaries.
func Func(fn interface{}) *FuncValue {
	fv := reflect.ValueOf(fn)
	ftype := fv.Type()
	if ftype.Kind() != reflect.Func {
		typecheck.Panicf(1, "bigslice.Func: argument to func is a %T, not a func", fn)
	}
	if ftype.NumOut() != 1 || ftype.Out(0) != typeOfSlice {
		typecheck.Panicf(1, "bigslice.Func: func must return a single bigslice.Slice")
	}
	v := new(FuncValue)
	v.fn = fv
	for i := 0; i < ftype.NumIn(); i++ {
		typ := ftype.In(i)
		v.args = append(v.args, typ)
		if typ.Kind() != reflect.Interface {
			gob.Register(reflect.Zero(typ).Interface())
		}
	}
	if atomic.AddInt32(&funcsBusy, 1) != 1 {
		panic("bigslice.Func: data race")
	}
	v.index = len(funcs)
	funcs = append(funcs, v)
	if atomic.AddInt32(&funcsBusy, -1) != 0 {
		panic("bigslice.Func: data race")
	}
	return v
}

// Invocation represents an invocation of a Bigslice func of the same
// binary. Invocations can be transmitted across process boundaries
// and thus may be invoked by remote executors.
//
// Each invocation carries an invocation index, which is a unique index
// for invocations within a process namespace. It can thus be used to
// represent a particular function invocation from a driver process.
//
// Invocations must be created by newInvocation.
type Invocation struct {
	Index uint64
	Func  uint64
	Args  []interface{}
}

var invocationIndex uint64

func newInvocation(fn uint64, args ...interface{}) Invocation {
	return Invocation{
		Index: atomic.AddUint64(&invocationIndex, 1),
		Func:  fn,
		Args:  args,
	}
}

// Invoke performs the Func invocation represented by this Invocation
// instance, returning the resulting slice.
func (i Invocation) Invoke() Slice {
	return funcs[i.Func].Apply(i.Args...)
}
