// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice_test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetest"
)

func reshuffleTest(t *testing.T, transform func(bigslice.Slice) bigslice.Slice) {
	t.Helper()
	const N = 100
	col1 := make([]lengthHashKey, N)
	col2 := make([]int, N)
	want := map[lengthHashKey][]int{} // map of val1 -> []val2
	rnd := rand.New(rand.NewSource(0))
	for i := range col1 {
		s := strings.Repeat("a", rnd.Intn(N/10)) // We want many strings of each length.
		col1[i] = lengthHashKey(s)
		col2[i] = rnd.Intn(100)
		want[col1[i]] = append(want[col1[i]], col2[i])
	}
	for _, vals := range want {
		sort.Ints(vals) // for equality test later
	}
	for m := 1; m < 10; m++ {
		t.Run(fmt.Sprint(m), func(t *testing.T) {
			slice := bigslice.Const(m, append([]lengthHashKey{}, col1...), append([]int{}, col2...))
			slice = transform(slice)

			// map of col1 length -> set of shards that had keys of that length.
			lengthShards := map[int]map[int]struct{}{}
			got := map[lengthHashKey][]int{} // map of val1 -> []val2
			var (
				val1 lengthHashKey
				val2 int
			)
			slice = bigslice.Scan(slice, func(shard int, scanner *sliceio.Scanner) error {
				for scanner.Scan(context.Background(), &val1, &val2) {
					if _, ok := lengthShards[len(val1)]; !ok {
						lengthShards[len(val1)] = map[int]struct{}{}
					}
					lengthShards[len(val1)][shard] = struct{}{}
					got[val1] = append(got[val1], val2)
				}
				return scanner.Err()
			})

			sess := exec.Start(exec.Local)
			defer sess.Shutdown()
			_, err := sess.Run(context.Background(), bigslice.Func(func() bigslice.Slice { return slice }))
			if err != nil {
				t.Fatalf("run error %v", err)
			}

			for length, shards := range lengthShards {
				if len(shards) != 1 {
					t.Errorf("found keys of length %d in multiple shards: %v", length, shards)
				}
			}

			for _, vals := range got {
				sort.Ints(vals)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

type lengthHashKey string

func init() {
	frame.RegisterOps(func(slice []lengthHashKey) frame.Ops {
		return frame.Ops{
			Less:         func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, _ uint32) uint32 { return uint32(len(slice[i])) },
		}
	})
}

func TestReshuffle(t *testing.T) {
	reshuffleTest(t, bigslice.Reshuffle)
}

func TestRepartition(t *testing.T) {
	reshuffleTest(t, func(slice bigslice.Slice) bigslice.Slice {
		return bigslice.Repartition(slice, func(nshard int, key lengthHashKey, value int) int {
			return len(key) % nshard
		})
	})
}

func TestRepartitionType(t *testing.T) {
	slice := bigslice.Const(1, []int{}, []string{})
	expectTypeError(t, "repartiton: expected func(int, int, string) int, got func() int", func() {
		bigslice.Repartition(slice, func() int { return 0 })
	})
	expectTypeError(t, "repartiton: expected func(int, int, string) int, got func(int, int, string)", func() {
		bigslice.Repartition(slice, func(_ int, _ int, _ string) {})
	})
}

func ExampleRepartition() {
	// Count rows per shard before and after using Repartition to get ideal
	// partitioning by taking advantage of the knowledge that our keys are
	// sequential integers.

	// countRowsPerShard is a utility that counts the number of rows per shard
	// and stores it in rowsPerShard.
	var rowsPerShard []int
	countRowsPerShard := func(numShards int, slice bigslice.Slice) bigslice.Slice {
		rowsPerShard = make([]int, numShards)
		return bigslice.WriterFunc(slice,
			func(shard int, _ struct{}, _ error, xs []int) error {
				rowsPerShard[shard] += len(xs)
				return nil
			},
		)
	}

	const numShards = 2
	slice := bigslice.Const(numShards, []int{1, 2, 3, 4, 5, 6})

	slice0 := countRowsPerShard(numShards, slice)
	fmt.Println("# default partitioning")
	fmt.Println("## slice contents")
	slicetest.Print(slice0)
	fmt.Println("## row count per shard")
	for shard, count := range rowsPerShard {
		fmt.Printf("shard:%d count:%d\n", shard, count)
	}

	slice1 := bigslice.Repartition(slice, func(nshard, x int) int {
		// We know our slice keys are sequential integers, so we partition
		// perfectly with mod.
		return x % nshard
	})
	slice1 = countRowsPerShard(numShards, slice1)
	fmt.Println("# repartitioned")
	// Note that the slice contents are unchanged.
	fmt.Println("## slice contents")
	slicetest.Print(slice1)
	// Note that the partitioning has changed.
	fmt.Println("## row count per shard")
	for shard, count := range rowsPerShard {
		fmt.Printf("shard:%d count:%d\n", shard, count)
	}
	// Output:
	// # default partitioning
	// ## slice contents
	// 1
	// 2
	// 3
	// 4
	// 5
	// 6
	// ## row count per shard
	// shard:0 count:4
	// shard:1 count:2
	// # repartitioned
	// ## slice contents
	// 1
	// 2
	// 3
	// 4
	// 5
	// 6
	// ## row count per shard
	// shard:0 count:3
	// shard:1 count:3
}

func ExampleReshard() {
	// Count rows per shard before and after using Reshard to change the number
	// of shards from 2 to 4.

	// countRowsPerShard is a utility that counts the number of rows per shard
	// and stores it in rowsPerShard.
	var rowsPerShard []int
	countRowsPerShard := func(numShards int, slice bigslice.Slice) bigslice.Slice {
		rowsPerShard = make([]int, numShards)
		return bigslice.WriterFunc(slice,
			func(shard int, _ struct{}, _ error, xs []int) error {
				rowsPerShard[shard] += len(xs)
				return nil
			},
		)
	}

	const beforeNumShards = 2
	slice := bigslice.Const(beforeNumShards, []int{1, 2, 3, 4, 5, 6})

	before := countRowsPerShard(beforeNumShards, slice)
	fmt.Println("# before")
	fmt.Println("## slice contents")
	slicetest.Print(before)
	fmt.Println("## row count per shard")
	for shard, count := range rowsPerShard {
		fmt.Printf("shard:%d count:%d\n", shard, count)
	}

	// Reshard to 4 shards.
	const afterNumShards = 4
	after := bigslice.Reshard(slice, afterNumShards)
	after = countRowsPerShard(afterNumShards, after)
	fmt.Println("# after")
	fmt.Println("## slice contents")
	slicetest.Print(after)
	fmt.Println("## row count per shard")
	for shard, count := range rowsPerShard {
		fmt.Printf("shard:%d count:%d\n", shard, count)
	}
	// Output:
	// # before
	// ## slice contents
	// 1
	// 2
	// 3
	// 4
	// 5
	// 6
	// ## row count per shard
	// shard:0 count:4
	// shard:1 count:2
	// # after
	// ## slice contents
	// 1
	// 2
	// 3
	// 4
	// 5
	// 6
	// ## row count per shard
	// shard:0 count:2
	// shard:1 count:1
	// shard:2 count:1
	// shard:3 count:2
}

func ExampleReshuffle() {
	// Count rows per shard before and after a Reshuffle, showing same-keyed
	// rows all go to the same shard.

	// countRowsPerShard is a utility that counts the number of rows per shard
	// and stores it in rowsPerShard.
	var rowsPerShard []int
	countRowsPerShard := func(numShards int, slice bigslice.Slice) bigslice.Slice {
		rowsPerShard = make([]int, numShards)
		return bigslice.WriterFunc(slice,
			func(shard int, _ struct{}, _ error, xs []int) error {
				rowsPerShard[shard] += len(xs)
				return nil
			},
		)
	}

	const numShards = 2
	slice := bigslice.Const(numShards, []int{1, 2, 3, 4, 5, 6})
	slice = bigslice.Map(slice, func(_ int) int { return 0 })

	before := countRowsPerShard(numShards, slice)
	fmt.Println("# before")
	fmt.Println("## slice contents")
	slicetest.Print(before)
	fmt.Println("## row count per shard")
	for shard, count := range rowsPerShard {
		fmt.Printf("shard:%d count:%d\n", shard, count)
	}

	after := bigslice.Reshuffle(slice)
	after = countRowsPerShard(numShards, after)
	fmt.Println("# after")
	// We set all our keys to 0. After reshuffling, all rows will be in the same
	// shard.
	fmt.Println("## slice contents")
	slicetest.Print(after)
	fmt.Println("## row count per shard")
	for shard, count := range rowsPerShard {
		fmt.Printf("shard:%d count:%d\n", shard, count)
	}
	// Output:
	// # before
	// ## slice contents
	// 0
	// 0
	// 0
	// 0
	// 0
	// 0
	// ## row count per shard
	// shard:0 count:4
	// shard:1 count:2
	// # after
	// ## slice contents
	// 0
	// 0
	// 0
	// 0
	// 0
	// 0
	// ## row count per shard
	// shard:0 count:6
	// shard:1 count:0
}
