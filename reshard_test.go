package bigslice_test

import (
	"context"
	"math/rand"
	"strings"
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/sliceio"
)

func TestReshard(t *testing.T) {
	// Construct a slice with columns:
	//   1. lengthHashKey (strings that hash to their length), and
	//   2. same value copied as a []byte
	const N = 100
	col1 := make([]lengthHashKey, N)
	col2 := make([][]byte, N)
	rnd := rand.New(rand.NewSource(0))
	for i := range col1 {
		s := strings.Repeat("a", rnd.Intn(N/10)) // We want many strings of each length.
		col1[i] = lengthHashKey(s)
		col2[i] = []byte(s)
	}
	for m := 1; m < 10; m++ {
		slice := bigslice.Const(m, append([]lengthHashKey{}, col1...), append([][]byte{}, col2...))
		slice = bigslice.Reshard(slice)

		// map of col1 length -> set of shards that had keys of that length.
		lengthShards := map[int]map[int]struct{}{}
		slice = bigslice.Scan(slice, func(shard int, scanner *sliceio.Scanner) error {
			var (
				val1 lengthHashKey
				val2 []byte
			)
			for scanner.Scan(context.Background(), &val1, &val2) {
				if string(val1) != string(val2) {
					t.Errorf("expected matching val1, val2, got %q, %q", val1, val2)
				}
				if _, ok := lengthShards[len(val1)]; !ok {
					lengthShards[len(val1)] = map[int]struct{}{}
				}
				lengthShards[len(val1)][shard] = struct{}{}
			}
			return scanner.Err()
		})
		_ = run(context.Background(), t, slice)

		for length, shards := range lengthShards {
			if len(shards) != 1 {
				t.Errorf("found keys of length %d in multiple shards: %v", length, shards)
			}
		}
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
