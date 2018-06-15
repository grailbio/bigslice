// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package kernel

import (
	"reflect"
	"sort"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/typecheck"
)

var (
	xtypeOfInt  = reflect.TypeOf(0)
	xtypeOfBool = reflect.TypeOf(false)
)

type lessSorter reflect.Value

// LessSorter constructs a Sorter from a function of the form
//
//	func(vals []someType) func(i, j int) bool
//
// that will then be used to sort and compare values of the type
// "someType".
//
// LessSorter panics if the value does not comply to the (schematic)
// function signature
//
//	<t>func([]t) func(i, j int) bool
//
// The returned Sorter can be registered as a kernel for type someType.
func LessSorter(lessFunc interface{}) Sorter {
	typ := reflect.TypeOf(lessFunc)
	check := func(ok bool) {
		if !ok {
			typecheck.Panicf(2, "expected func([]t) func(i, j) bool, got %s", typ)
		}
	}
	check(typ.Kind() == reflect.Func)
	check(typ.NumIn() == 1)
	check(typ.NumOut() == 1)
	arg, ret := typ.In(0), typ.Out(0)
	check(arg.Kind() == reflect.Slice)
	check(ret.Kind() == reflect.Func)
	check(ret.NumIn() == 2 && ret.NumOut() == 1)
	check(ret.In(0) == xtypeOfInt && ret.In(1) == xtypeOfInt)
	check(ret.Out(0) == xtypeOfBool)
	return lessSorter(reflect.ValueOf(lessFunc))
}

func (val lessSorter) Sort(f frame.Frame) {
	less := reflect.Value(val).Call([]reflect.Value{f[0].Value()})[0].Interface().(func(i, j int) bool)
	sortFrame(f, less)
}

func (val lessSorter) Less(f frame.Frame, i int, g frame.Frame, j int) bool {
	// TODO(marius): this is not ideal; rethink the Less interface.
	keys := reflect.MakeSlice(reflect.SliceOf(f.Out(0)), 2, 2)
	keys.Index(0).Set(f[0].Index(i))
	keys.Index(1).Set(g[0].Index(j))
	less := reflect.Value(val).Call([]reflect.Value{keys})[0].Interface().(func(i, j int) bool)
	return less(0, 1)
}

func (val lessSorter) IsSorted(f frame.Frame) bool {
	less := reflect.Value(val).Call([]reflect.Value{f[0].Value()})[0].Interface().(func(i, j int) bool)
	return sort.SliceIsSorted(f[0].Interface(), less)
}
