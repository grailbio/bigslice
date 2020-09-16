package bigslice_test

import (
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice"
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
