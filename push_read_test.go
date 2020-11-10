package bigslice_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/base/must"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/sliceio"
)

func TestPushReader(t *testing.T) {
	const (
		N      = 1000
		Nshard = 10
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
//   BenchmarkReaders/heavy=false/n=10/standard-8                 100          10641845 ns/op
//   BenchmarkReaders/heavy=false/n=10/push-8                     100          11062321 ns/op
//   BenchmarkReaders/heavy=false/n=1000/standard-8                12          98823548 ns/op
//   BenchmarkReaders/heavy=false/n=1000/push-8                     9         117393717 ns/op
//   BenchmarkReaders/heavy=true/n=10/standard-8                   20          55009760 ns/op
//   BenchmarkReaders/heavy=true/n=10/push-8                       20          56645978 ns/op
//   BenchmarkReaders/heavy=true/n=1000/standard-8                  1        4544902923 ns/op
//   BenchmarkReaders/heavy=true/n=1000/push-8                      1        4555043499 ns/op
//   PASS
//   ok      github.com/grailbio/bigslice    26.135s
func BenchmarkReaders(b *testing.B) {
	ctx := context.Background()
	sess := exec.Start(exec.Local)
	for _, heavyWork := range []bool{false, true} {
		for _, rowsPerShard := range []int{10, 1000} {
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
			opts := benchmarkOpts{
				Seed:         1,
				RowsPerShard: rowsPerShard,
				HeavyWork:    heavyWork,
			}
			b.Run(fmt.Sprintf("heavy=%t/n=%d/standard", heavyWork, rowsPerShard), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					checkResult(sess.Must(ctx, benchmarkPushReader, opts))
				}
			})
			opts.PushReader = true
			b.Run(fmt.Sprintf("heavy=%t/n=%d/push", heavyWork, rowsPerShard), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					checkResult(sess.Must(ctx, benchmarkPushReader, opts))
				}
			})
		}
	}
	sess.Shutdown()
}

type benchmarkOpts struct {
	Seed         int64
	RowsPerShard int
	PushReader   bool
	HeavyWork    bool
}

var benchmarkPushReader = bigslice.Func(func(opts benchmarkOpts) bigslice.Slice {
	const nShards = 100
	shardSeeds := make([]int64, nShards)
	rnd := rand.New(rand.NewSource(opts.Seed))
	for i := range shardSeeds {
		shardSeeds[i] = rnd.Int63()
	}
	var slice bigslice.Slice
	if opts.PushReader {
		slice = bigslice.PushReader(nShards, func(shard int, row func(int32)) error {
			shardRnd := rand.New(rand.NewSource(shardSeeds[shard]))
			for i := 0; i < opts.RowsPerShard; i++ {
				if opts.HeavyWork {
					row(heavyWork(shardRnd))
				} else {
					row(lightWork(shardRnd))
				}
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
			if state.doneRows == opts.RowsPerShard {
				return 0, sliceio.EOF
			}
			var i int
			for state.doneRows < opts.RowsPerShard && i < len(nums) {
				if opts.HeavyWork {
					nums[i] = heavyWork(state.Rand)
				} else {
					nums[i] = lightWork(state.Rand)
				}
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

func heavyWork(r *rand.Rand) int32 {
	for i := 0; i < 10000; i++ {
		_ = r.Int()
	}
	return lightWork(r)
}

func lightWork(r *rand.Rand) int32 {
	return r.Int31()
}
