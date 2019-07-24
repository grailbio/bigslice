// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"encoding/gob"
	"fmt"
	"reflect"
	"runtime"
	"strings"
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
	fn        reflect.Value
	args      []reflect.Type
	index     int
	exclusive bool

	// file and line are the location at which the function was defined.
	file string
	line int
}

// Exclusive marks this func to require mutually exclusive machine
// allocation.
//
// NOTE: This is an experimental API that may change.
func (f *FuncValue) Exclusive() *FuncValue {
	fv := new(FuncValue)
	*fv = *f
	fv.exclusive = true
	return fv
}

// NumIn returns the number of input arguments to f.
func (f *FuncValue) NumIn() int { return len(f.args) }

// In returns the i'th argument type of function f.
func (f *FuncValue) In(i int) reflect.Type { return f.args[i] }

// Invocation creates an invocation representing the function f
// applied to the provided arguments. Invocation panics with a type
// error if the provided arguments do not match in type or arity.
func (f *FuncValue) Invocation(location string, args ...interface{}) Invocation {
	argTypes := make([]reflect.Type, len(args))
	for i, arg := range args {
		argTypes[i] = reflect.TypeOf(arg)
	}
	f.typecheck(argTypes...)
	return newInvocation(location, uint64(f.index), f.exclusive, args...)
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
	_, v.file, v.line, _ = runtime.Caller(1)
	return v
}

// FuncLocations returns a slice of strings that describe the locations of
// Func creation, in the same order as the Funcs registry. We use this to
// verify that worker processes have the same Funcs. Note that this is not a
// precisely correct verification, as it's possible to define multiple Funcs on
// the same line. However, it's good enough for the scenarios we have
// encountered or anticipate.
func FuncLocations() []string {
	locs := make([]string, len(funcs))
	for i, f := range funcs {
		locs[i] = fmt.Sprintf("%s:%d", f.file, f.line)
	}
	return locs
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
	Index     uint64
	Func      uint64
	Args      []interface{}
	Exclusive bool
	Location  string
}

func (inv Invocation) String() string {
	args := make([]string, len(inv.Args))
	for i, arg := range args {
		args[i] = fmt.Sprint(arg)
	}
	return fmt.Sprintf("%s(%d, %d) %s", inv.Location, inv.Func, inv.Index, strings.Join(args, " "))
}

var invocationIndex uint64

func newInvocation(location string, fn uint64, exclusive bool, args ...interface{}) Invocation {
	return Invocation{
		Index:     atomic.AddUint64(&invocationIndex, 1),
		Func:      fn,
		Args:      args,
		Exclusive: exclusive,
		Location:  location,
	}
}

// Invoke performs the Func invocation represented by this Invocation
// instance, returning the resulting slice.
func (i Invocation) Invoke() Slice {
	return funcs[i.Func].Apply(i.Args...)
}

// FuncLocationsDiff returns a slice of strings that describes the differences
// between lhs and rhs locations slices as returned by FuncLocations. The slice
// is a unified diff between the slices, so if you print each element on a
// line, you'll get interpretable output. For example:
//
//  for _, edit := FuncLocationsDiff([]string{"a", "b", "c"}, []string{"a", "c"}) {
//      fmt.Println(edit)
//  }
//
// will produce:
//
//  a
//  - b
//  c
//
// If the slices are identical, it returns nil.
func FuncLocationsDiff(lhs, rhs []string) []string {
	// This is a vanilla Levenshtein distance implementation.
	const (
		editNone = iota
		editAdd
		editDel
	)
	type cell struct {
		edit int
		cost int
	}
	cells := make([][]cell, len(lhs)+1)
	for i := range cells {
		cells[i] = make([]cell, len(rhs)+1)
	}
	for i := 1; i < len(lhs)+1; i++ {
		cells[i][0].edit = editDel
		cells[i][0].cost = i
	}
	for j := 1; j < len(rhs)+1; j++ {
		cells[0][j].edit = editAdd
		cells[0][j].cost = j
	}
	for i := 1; i < len(lhs)+1; i++ {
		for j := 1; j < len(rhs)+1; j++ {
			switch {
			case lhs[i-1] == rhs[j-1]:
				cells[i][j].cost = cells[i-1][j-1].cost
			// No replacement, as we want to represent it as
			// deletion-then-addition in our unified diff output anyway.
			case cells[i-1][j].cost < cells[i][j-1].cost:
				cells[i][j].edit = editDel
				cells[i][j].cost = cells[i-1][j].cost + 1
			default:
				cells[i][j].edit = editAdd
				cells[i][j].cost = cells[i][j-1].cost + 1
			}
		}
	}
	var (
		d      []string
		differ bool
	)
	for i, j := len(lhs), len(rhs); i > 0 || j > 0; {
		switch cells[i][j].edit {
		case editNone:
			d = append(d, lhs[i-1])
			i -= 1
			j -= 1
		case editAdd:
			d = append(d, "+ "+rhs[j-1])
			j -= 1
			differ = true
		case editDel:
			d = append(d, "- "+lhs[i-1])
			i -= 1
			differ = true
		}
	}
	if !differ {
		return nil
	}
	for i := len(d)/2 - 1; i >= 0; i-- {
		opp := len(d) - 1 - i
		d[i], d[opp] = d[opp], d[i]
	}
	return d
}
