// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"reflect"
	"testing"

	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/bigslice"
)

func rangeSlice(i, j int) []int {
	s := make([]int, j-i)
	for k := range s {
		s[k] = i + k
	}
	return s
}

func TestSessionIterative(t *testing.T) {
	var nvalues, nadd int
	values := bigslice.Func(func() bigslice.Slice {
		nvalues++
		return bigslice.Const(5, rangeSlice(0, 1000))
	})
	add := bigslice.Func(func(x int, slice bigslice.Slice) bigslice.Slice {
		return bigslice.Map(slice, func(i int) int {
			nadd++
			return i + x
		})
	})
	var (
		ctx  = context.Background()
		nrun int
	)
	testSession(t, func(sess *Session) {
		nrun++
		res, err := sess.Run(ctx, values)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 5; i++ {
			res, err = sess.Run(ctx, add, i, res)
			if err != nil {
				t.Fatal(err)
			}
		}
		var (
			scan = res.Scan(ctx)
			ints []int
			x    int
		)
		for scan.Scan(ctx, &x) {
			ints = append(ints, x)
		}
		if err := scan.Err(); err != nil {
			t.Fatal(err)
		}
		if got, want := ints, rangeSlice(10, 1010); !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	if got, want := nvalues, nrun+1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := nadd, nrun*5*1000; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

var executors = map[string]Option{
	"Local":           Local,
	"Bigmachine.Test": Bigmachine(testsystem.New()),
}

func testSession(t *testing.T, run func(sess *Session)) {
	for _, opt := range executors {
		sess := Start(opt)
		run(sess)
	}
}
