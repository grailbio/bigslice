// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package bigslice_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetest"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/testutil"
)

func TestCache(t *testing.T) {
	makeSlice := func(n, nShard int, dir string, computeAllowed bool) bigslice.Slice {
		input := make([]int, n)
		for i := range input {
			input[i] = i
		}
		slice := bigslice.Const(nShard, input)
		slice = bigslice.Map(slice, func(i int) int {
			if !computeAllowed {
				panic("compute not allowed")
			}
			return i * 2
		})
		ctx := context.Background()
		slice = bigslice.Cache(ctx, slice, filepath.Join(dir, "cached"))
		return slice
	}
	runTestCache(t, makeSlice)
}

// TestCacheDeps verifies that caching works when pipelined tasks have non-empty
// dependencies. When the cache is valid, we do not need to read from these
// dependencies. Verify that this does not break compilation or execution (e.g.
// empty dependencies given to tasks that expect non-empty dependencies).
func TestCacheDeps(t *testing.T) {
	exec.DoShuffleReaders = false
	makeSlice := func(n, nShard int, dir string, computeAllowed bool) bigslice.Slice {
		input := make([]int, n)
		for i := range input {
			input[i] = i
		}
		slice := bigslice.Const(nShard, input)
		// This shuffle causes a break in the pipeline, so the pipelined task
		// will have a dependency on the Const slice tasks. Caching should cause
		// compilation/execution to eliminate these dependencies safely.
		slice = bigslice.Reshuffle(slice)
		slice = bigslice.Map(slice, func(i int) int {
			if !computeAllowed {
				panic("compute not allowed")
			}
			return i * 2
		})
		ctx := context.Background()
		slice = bigslice.Cache(ctx, slice, filepath.Join(dir, "cached"))
		return slice
	}
	runTestCache(t, makeSlice)
}

// runTestCache verifies that the caching in the slice returned by makeSlice
// behaves as expected. See usage in TestCache.
func runTestCache(t *testing.T, makeSlice func(n, nShard int, dir string, computeAllowed bool) bigslice.Slice) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()

	const (
		N      = 10000
		Nshard = 10
	)
	slice1 := makeSlice(N, Nshard, dir, true)
	if got, want := len(ls1(t, dir)), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	scan1 := runLocal(ctx, t, slice1)
	defer scan1.Close()
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}

	// Recompute the slice to pick up the cached results.
	slice2 := makeSlice(N, Nshard, dir, false)
	scan2 := runLocal(ctx, t, slice2)
	defer scan2.Close()
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}

	v1 := scanInts(ctx, t, scan1)
	v2 := scanInts(ctx, t, scan2)
	if got, want := len(v1), N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if !reflect.DeepEqual(v1, v2) {
		t.Errorf("corrupt cache")
	}
}

func TestCacheIncremental(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()

	const (
		N      = 10000
		Nshard = 10
	)

	rowsRan := make([]bool, N)

	input := make([]int, N)
	for i := range input {
		input[i] = i
	}
	makeSlice := func() bigslice.Slice {
		slice := bigslice.Const(Nshard, input)
		slice = bigslice.Map(slice, func(i int) int {
			rowsRan[i] = true
			return i * 2
		})
		slice = bigslice.Cache(ctx, slice, filepath.Join(dir, "cached"))
		return slice
	}

	// Run and populate the cache.
	_ = runLocal(ctx, t, makeSlice())
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}

	// Run and ensure there's no new computation.
	for i := range rowsRan {
		rowsRan[i] = false
	}
	_ = runLocal(ctx, t, makeSlice())
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}
	for _, ran := range rowsRan {
		if ran {
			t.Error("want cache use")
		}
	}

	// Delete some cache entries and ensure there's recomputation.
	for i, f := range ls1(t, dir) {
		if i%2 == 0 {
			continue
		}
		if err := os.Remove(filepath.Join(dir, f)); err != nil {
			t.Error(err)
		}
	}
	for i := range rowsRan {
		rowsRan[i] = false
	}
	_ = runLocal(ctx, t, makeSlice())
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}
	var nRans int
	for _, ran := range rowsRan {
		if ran {
			nRans++
		}
	}
	if nRans < Nshard {
		t.Error("want all recompution")
	}
}

func TestCachePartialIncremental(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()

	const (
		N      = 10000
		Nshard = 10
	)

	rowsRan := make([]bool, N)

	input := make([]int, N)
	for i := range input {
		input[i] = i
	}
	makeSlice := func() bigslice.Slice {
		slice := bigslice.Const(Nshard, input)
		slice = bigslice.Map(slice, func(i int) int {
			rowsRan[i] = true
			return i * 2
		})
		slice = bigslice.CachePartial(ctx, slice, filepath.Join(dir, "cached"))
		return slice
	}

	// Run and populate the cache.
	_ = runLocal(ctx, t, makeSlice())
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}

	// Run and ensure there's no new computation.
	for i := range rowsRan {
		rowsRan[i] = false
	}
	_ = runLocal(ctx, t, makeSlice())
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}
	for _, ran := range rowsRan {
		if ran {
			t.Error("want cache use")
		}
	}

	// Delete some cache entries and ensure there's partial recomputation.
	for i, f := range ls1(t, dir) {
		if i%2 == 0 {
			continue
		}
		if err := os.Remove(filepath.Join(dir, f)); err != nil {
			t.Error(err)
		}
	}
	for i := range rowsRan {
		rowsRan[i] = false
	}
	_ = runLocal(ctx, t, makeSlice())
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}
	var nRowsRan int
	for _, ran := range rowsRan {
		if ran {
			nRowsRan++
		}
	}
	if nRowsRan == 0 || nRowsRan >= N {
		t.Errorf("want partial recomputation, got %d of %d rows", nRowsRan, N)
	}
}

func TestCacheErr(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()

	computeRan := false

	makeSlice := func() bigslice.Slice {
		slice := bigslice.ReaderFunc(1, func(shard int, state *bool, ints []int) (n int, err error) {
			if *state {
				return 0, errors.New("random error")
			}
			for i := range ints {
				ints[i] = i
			}
			*state = true
			computeRan = true
			return len(ints), nil
		})
		slice = bigslice.Cache(ctx, slice, file.Join(dir, "cached"))
		return slice
	}
	if err := slicetest.RunErr(makeSlice()); err == nil {
		t.Error("expected error")
	}
	if !computeRan {
		t.Error()
	}
	// Ensure computation is rerun after error.
	if err := slicetest.RunErr(makeSlice()); err == nil {
		t.Error("expected error")
	}
	if !computeRan {
		t.Error()
	}
}

// TestReadCache verifies that ReadCache successfully reads from an existing cache.
func TestReadCache(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	prefix := filepath.Join(dir, "cached")
	ctx := context.Background()

	const (
		N      = 10000
		Nshard = 10
	)
	input := make([]int, N)
	for i := range input {
		input[i] = i
	}
	slice1 := bigslice.Const(Nshard, input)
	slice1 = bigslice.Cache(ctx, slice1, prefix)
	scan1 := runLocal(ctx, t, slice1)
	defer scan1.Close()

	// We now have a populated cache. Read from it, and make sure we get the
	// same results.
	slice2 := bigslice.ReadCache(ctx, slice1, slice1.NumShard(), prefix)
	scan2 := runLocal(ctx, t, slice2)

	v1 := scanInts(ctx, t, scan1)
	v2 := scanInts(ctx, t, scan2)
	if got, want := len(v1), N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if !reflect.DeepEqual(v1, v2) {
		t.Errorf("corrupt cache")
	}
}

// TestReadCacheError verifies that a ReadCache reader returns an error if the
// cache does not exist.
func TestReadCacheError(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	var (
		prefix = filepath.Join(dir, "cached")
		ctx    = context.Background()
		slice  = bigslice.ReadCache(ctx, slicetype.New(reflect.TypeOf(0)), 1, prefix)
		fn     = bigslice.Func(func() bigslice.Slice { return slice })
		sess   = exec.Start(exec.Local)
	)
	defer sess.Shutdown()
	_, err := sess.Run(ctx, fn)
	if err == nil {
		t.Errorf("expected error when reading from non-existent cache")
	}
}

func ls1(t *testing.T, dir string) []string {
	t.Helper()
	d, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	infos, err := d.Readdir(-1)
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]string, len(infos))
	for i := range paths {
		paths[i] = infos[i].Name()
	}
	sort.Strings(paths)
	return paths
}

func runLocal(ctx context.Context, t *testing.T, slice bigslice.Slice) *sliceio.Scanner {
	t.Helper()
	fn := bigslice.Func(func() bigslice.Slice { return slice })
	sess := exec.Start(exec.Local)
	defer sess.Shutdown()
	res, err := sess.Run(ctx, fn)
	if err != nil {
		t.Fatalf("error running func: %v", err)
	}
	return res.Scanner()
}

func scanInts(ctx context.Context, t *testing.T, scan *sliceio.Scanner) []int {
	t.Helper()
	var (
		v  int
		vs []int
	)
	for scan.Scan(ctx, &v) {
		vs = append(vs, v)
	}
	if err := scan.Err(); err != nil {
		t.Fatalf("scan error: %v", err)
	}
	sort.Ints(vs)
	return vs
}
