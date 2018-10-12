// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package slicetest provides utilities for testing Bigslice user code.
// The utilities here are generally not optimized for performance or
// robustness; they are strictly intended for unit testing.
package slicetest

import (
	"context"
	"reflect"
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/sliceio"
)

// Run evaluates the provided slice in local execution mode,
// returning a scanner for the result. Errors are reported as fatal
// to the provided t instance. Run is intended for unit testing of
// Slice implementations.
func Run(t *testing.T, slice bigslice.Slice) *sliceio.Scanner {
	t.Helper()
	ctx := context.Background()
	fn := bigslice.Func(func() bigslice.Slice { return slice })
	sess := exec.Start(exec.Local)
	res, err := sess.Run(ctx, fn)
	if err != nil {
		t.Fatal(err)
	}
	return res.Scan(ctx)
}

// ScanAll scans all entries from the scanner into the provided
// columns, which must be pointers to slices of the correct column
// types. For example, to read all values for a Slice<int, string>:
//
//	var (
//		ints []int
//		strings []string
//	)
//	ScanAll(test, scan, &ints, &strings)
//
// Errors are reported as fatal to the provided t instance.
func ScanAll(t *testing.T, scan *sliceio.Scanner, cols ...interface{}) {
	t.Helper()
	vs := make([]reflect.Value, len(cols))
	elemTypes := make([]reflect.Type, len(cols))
	for i := range vs {
		vs[i] = reflect.Indirect(reflect.ValueOf(cols[i]))
		vs[i].Set(vs[i].Slice(0, 0))
		elemTypes[i] = vs[i].Type().Elem()
	}
	ctx := context.Background()
	args := make([]interface{}, len(cols))
	for n := 0; ; n++ {
		for i := range vs {
			vs[i].Set(reflect.Append(vs[i], reflect.Zero(elemTypes[i])))
			args[i] = vs[i].Index(n).Addr().Interface()
		}
		if !scan.Scan(ctx, args...) {
			for i := range vs {
				vs[i].Set(vs[i].Slice(0, n))
			}
			break
		}
	}
	if err := scan.Err(); err != nil {
		t.Fatal(err)
	}
}

// RunAndScan evaluates the provided slice and scans its results into
// the provided slice pointers. Errors are reported as fatal to the provided
// t instance.
func RunAndScan(t *testing.T, slice bigslice.Slice, cols ...interface{}) {
	t.Helper()
	ScanAll(t, Run(t, slice), cols...)
}
