// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"reflect"
	"testing"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
)

func init() {
	log.AddFlags()
}

func rangeSlice(i, j int) []int {
	s := make([]int, j-i)
	for k := range s {
		s[k] = i + k
	}
	return s
}

func TestSessionIterative(t *testing.T) {
	const (
		Nelem  = 1000
		Nshard = 5
		Niter  = 5
	)
	var nvalues, nadd int
	values := bigslice.Func(func() bigslice.Slice {
		return bigslice.ReaderFunc(Nshard, func(shard int, n *int, out []int) (int, error) {
			beg, end := shardRange(Nelem, Nshard, shard)
			beg += *n
			t.Logf("shard %d beg %d end %d n %d", shard, beg, end, *n)
			if beg >= end { // empty or done
				nvalues++
				return 0, sliceio.EOF
			}
			m := copy(out, rangeSlice(beg, end))
			*n += m
			return m, nil
		})
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
	testSession(t, func(t *testing.T, sess *Session) {
		nrun++
		res, err := sess.Run(ctx, values)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < Niter; i++ {
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
	if got, want := nvalues, nrun*Nshard; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := nadd, nrun*Niter*1000; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

var executors = map[string]Option{
	"Local":           Local,
	"Bigmachine.Test": Bigmachine(testsystem.New()),
}

func testSession(t *testing.T, run func(t *testing.T, sess *Session)) {
	t.Helper()
	for name, opt := range executors {
		t.Run(name, func(t *testing.T) {
			sess := Start(opt)
			run(t, sess)
		})
	}
}

// shardRange gives the range covered by a shard.
func shardRange(nelem, nshard, shard int) (beg, end int) {
	elemsPerShard := (nelem + nshard - 1) / nshard
	beg = elemsPerShard * shard
	if beg >= nelem {
		beg = 0
		return
	}
	end = beg + elemsPerShard
	if end > nelem {
		end = nelem
	}
	return
}
