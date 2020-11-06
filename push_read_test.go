package bigslice_test

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/base/must"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/sliceio"
)

func TestPushReader(t *testing.T) {
	const (
		N      = 3
		Nshard = 1
	)
	slice := bigslice.PushReader(Nshard, func(shard int, sink func(string, int)) error {
		fuzzer := fuzz.NewWithSeed(1)
		var row struct {
			string
			int
		}
		for i := 0; i < N; i++ {
			fuzzer.Fuzz(&row)
			sink(row.string, row.int)
		}
		return nil
	})
	// Map everything to the same key so we can count them.
	slice = bigslice.Map(slice, func(s string, i int) (key string, count int) { return "", 1 })
	slice = bigslice.Fold(slice, func(a, e int) int { return a + e })
	assertEqual(t, slice, false, []string{""}, []int{N * Nshard})
}

// On an m5d.2xlarge on EC2:
//   goos: linux
//   goarch: amd64
//   pkg: github.com/grailbio/bigslice
//   BenchmarkReaders/10/pushreader-8                     110          10705539 ns/op
//   BenchmarkReaders/10/readerfunc-8                     100          10036689 ns/op
//   BenchmarkReaders/1000/pushreader-8                     8         131828675 ns/op
//   BenchmarkReaders/1000/readerfunc-8                    12          97414219 ns/op
//   BenchmarkReaders/100000/pushreader-8                   1        11181803529 ns/op
//   BenchmarkReaders/100000/readerfunc-8                   1        8714653324 ns/op
//   PASS
//   ok      github.com/grailbio/bigslice    26.135s
func BenchmarkReaders(b *testing.B) {
	ctx := context.Background()
	sess := exec.Start(exec.Local)
	for _, rowsPerShard := range []int{10, 1000, 100000} {
		lastResult := -1
		checkResult := func(sliceResult *exec.Result) {
			scanner := sliceResult.Scanner()
			var result int
			must.True(scanner.Scan(ctx, &result))
			if lastResult < 0 {
				lastResult = result
			}
			must.True(lastResult == result)
		}

		b.Run(strconv.Itoa(rowsPerShard), func(b *testing.B) {
			b.Run("pushreader", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					checkResult(sess.Must(ctx, benchmarkPushReader, int64(1), rowsPerShard, true))
				}
			})
			b.Run("readerfunc", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					checkResult(sess.Must(ctx, benchmarkPushReader, int64(1), rowsPerShard, false))
				}
			})
		})
	}
	sess.Shutdown()
}

var benchmarkPushReader = bigslice.Func(func(
	seed int64,
	rowsPerShard int,
	usePush bool,
) bigslice.Slice {
	const nShards = 100
	shardSeeds := make([]int64, nShards)
	rnd := rand.New(rand.NewSource(seed))
	for i := range shardSeeds {
		shardSeeds[i] = rnd.Int63()
	}
	var slice bigslice.Slice
	if usePush {
		slice = bigslice.PushReader(nShards, func(shard int, row func(int32)) error {
			shardRnd := rand.New(rand.NewSource(shardSeeds[shard]))
			for i := 0; i < rowsPerShard; i++ {
				row(shardRnd.Int31())
			}
			return nil
		})
	} else {
		type state struct {
			*rand.Rand
			doneRows int
		}
		slice = bigslice.ReaderFunc(nShards, func(shard int, state *state, nums []int32) (int, error) {
			if state.Rand == nil {
				state.Rand = rand.New(rand.NewSource(shardSeeds[shard]))
			}
			if state.doneRows == rowsPerShard {
				return 0, sliceio.EOF
			}
			var i int
			for state.doneRows < rowsPerShard && i < len(nums) {
				nums[i] = state.Rand.Int31()
				state.doneRows++
				i++
			}
			return i, nil
		})
	}
	slice = bigslice.Map(slice, func(num int32) (joinKey int, _ int32) {
		return 0, num
	})
	slice = bigslice.Fold(slice, func(accum int, num int32) int {
		if num < math.MaxInt32/2 {
			return accum
		}
		return accum + 1
	})
	return bigslice.Map(slice, func(joinKey, accum int) int {
		return accum
	})
})
