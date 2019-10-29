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
)

func TestReshuffle(t *testing.T) {
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
			slice = bigslice.Reshuffle(slice)

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
