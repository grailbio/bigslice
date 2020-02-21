// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package slicetest_test

import (
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/slicetest"
)

var words = []string{"few", "espy", "longer", "until", "interesting",
	"thus", "bason", "passage", "classes", "straighten",
	"ill", "property", "combine", "promise", "Chicago",
	"generally", "yellow", "per", "verb", "products",
	"file", "park", "doughty", "changed", "inaureoled",
	"flummoxed", "knife", "ghyll", "none", "bulwark",
	"provide", "background", "purposes", "bivouac", "removed",
	"jr", "adopt", "oil", "clean", "dingle",
	"experience", "population", "coomb", "slightly", "encourage",
	"kill", "mark", "present", "system", "standards",
	"rapid", "mean", "conditions", "control", "within",
	"circlet", "proper", "nothing", "craven", "case",
	"day", "serious", "might", "sound", "hadn't",
	"student", "rhode", "yammer", "caught", "deem",
	"homes", "marry", "stands", "elect", "modern",
	"activity", "servant", "too", "sport", "block",
	"low", "addition", "London", "accept", "unusual",
	"commercial", "grind", "books", "countries", "collect",
	"these", "marriage", "narrow", "honor", "gave",
	"lip", "country", "spring", "watching", "sea",
}

func randString(r *rand.Rand, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString(words[r.Intn(len(words))])
	}
	return b.String()
}

func randStringSlice(r *rand.Rand, n int) []string {
	strs := make([]string, n)
	for i := range strs {
		strs[i] = randString(r, r.Intn(5))
	}
	return strs
}

func randIntSlice(r *rand.Rand, n int) []int {
	ints := make([]int, n)
	for i := range ints {
		ints[i] = r.Int()
	}
	return ints
}

var runAndScan = bigslice.Func(func(strs []string, ints []int) bigslice.Slice {
	return bigslice.Const(10, strs, ints)
})

func TestRunAndScan(t *testing.T) {
	const N = 10000
	var (
		r    = rand.New(rand.NewSource(0))
		strs = randStringSlice(r, N)
		ints = randIntSlice(r, N)
	)
	var (
		scannedStrs []string
		scannedInts []int
	)
	args := []interface{}{strs, ints}
	slicetest.RunAndScan(t, runAndScan, args, &scannedStrs, &scannedInts)
	if got, want := len(scannedStrs), N; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := len(scannedInts), N; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	type row struct {
		S string
		I int
	}
	sortRows := func(rows []row) {
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].S == rows[j].S {
				return rows[i].I < rows[j].I
			}
			return rows[i].S < rows[j].S
		})
	}
	want := make([]row, N)
	for i := range want {
		want[i].S = strs[i]
		want[i].I = ints[i]
	}
	got := make([]row, N)
	for i := range got {
		got[i].S = scannedStrs[i]
		got[i].I = scannedInts[i]
	}
	sortRows(got)
	sortRows(want)
	if !reflect.DeepEqual(got, want) {
		t.Error("rows do not match")
	}
}
