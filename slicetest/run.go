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
	"sync"
	"testing"

	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/sliceio"
)

var sessOnce sync.Once
var sess *exec.Session

// getSess returns the session to use for tests. It is created on the first
// call.
func getSess() *exec.Session {
	sessOnce.Do(func() {
		sess = exec.Start(exec.Bigmachine(bigmachine.Local))
	})
	return sess
}

// Run evaluates the provided slice using a local bigmachine system, returning a
// scanner for the result. Errors are reported as fatal to the provided t
// instance. Run is intended for unit testing of Func/Slice implementations.
func Run(t *testing.T, fn *bigslice.FuncValue, args ...interface{}) *sliceio.Scanner {
	t.Helper()
	ctx := context.Background()
	res, err := getSess().Run(ctx, fn, args...)
	if err != nil {
		t.Fatal(err)
	}
	return res.Scanner()
}

// RunErr evaluates the provided Func using a local bigmachine system and
// returns the error, if any.
func RunErr(fn *bigslice.FuncValue, args ...interface{}) error {
	ctx := context.Background()
	_, err := getSess().Run(ctx, fn, args...)
	return err
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

// RunAndScan evaluates the provided Func and scans its results into the
// provided slice pointers. Errors are reported as fatal to the provided t
// instance.
func RunAndScan(t *testing.T, fn *bigslice.FuncValue, args []interface{}, cols ...interface{}) {
	t.Helper()
	scanner := Run(t, fn, args...)
	defer scanner.Close()
	ScanAll(t, scanner, cols...)
}
