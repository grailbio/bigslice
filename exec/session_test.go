// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/frame"
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

func TestSessionReuse(t *testing.T) {
	const N = 1000
	input := bigslice.Func(func() bigslice.Slice {
		return bigslice.Const(5, rangeSlice(0, 1000))
	})
	var nmap int
	mapper := bigslice.Func(func(slice bigslice.Slice) bigslice.Slice {
		return bigslice.Map(slice, func(i int) (int, int, int) {
			nmap++
			return i, i, i
		})
	})
	reducer := bigslice.Func(func(slice bigslice.Slice) bigslice.Slice {
		slice = bigslice.Map(slice, func(x, y, z int) (int, int, int) { return 0, y / 2, z })
		slice = bigslice.Prefixed(slice, 2)
		slice = bigslice.Reduce(slice, func(a, e int) int { return a + e })
		slice = bigslice.Map(slice, func(k1, k2, v int) (int, int) { return k2, v })
		return slice
	})
	unmap := bigslice.Func(func(slice bigslice.Slice) bigslice.Slice {
		return bigslice.Map(slice, func(x, y, z int) (int, int) { return x, y + z })
	})
	ctx := context.Background()
	testSession(t, func(t *testing.T, sess *Session) {
		nmap = 0
		input := sess.Must(ctx, input)
		mapped := sess.Must(ctx, mapper, input)
		var wg sync.WaitGroup
		var reduced *Result
		wg.Add(1)
		go func() {
			reduced = sess.Must(ctx, reducer, mapped)
			wg.Done()
		}()
		unmapped := sess.Must(ctx, unmap, mapped)
		wg.Wait()
		// The map results were reused:
		if got, want := nmap, N; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		// And we computed the correct results:
		var (
			f = readFrame(t, reduced, N/2)
			k = f.Interface(0).([]int)
			v = f.Interface(1).([]int)
		)
		for i := range k {
			if got, want := v[i], k[i]*4+1; got != want {
				t.Errorf("index %d: got %v, want %v", i, got, want)
			}
		}

		f = readFrame(t, unmapped, N)
		k = f.Interface(0).([]int)
		v = f.Interface(1).([]int)
		for i := range k {
			if got, want := v[i], k[i]*2; got != want {
				t.Errorf("index %d: got %v, want %v", i, got, want)
			}
		}
	})
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

func readFrame(t *testing.T, res *Result, n int) frame.Frame {
	t.Helper()
	f := frame.Make(res, n+1, n+1)
	ctx := context.Background()
	m, err := sliceio.ReadFull(ctx, res.Scan(ctx).Reader, f)
	if err != sliceio.EOF {
		t.Fatal(err)
	}
	if got, want := m, n; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	return f.Slice(0, n)
}
