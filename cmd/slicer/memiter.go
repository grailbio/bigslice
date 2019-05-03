// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
)

var memiterBaseline = bigslice.Func(func(n int, size int) (slice bigslice.Slice) {
	slice = bigslice.Const(1, make([]int, n))
	slice = bigslice.Map(slice, func(i int) []byte {
		return make([]byte, size)
	})
	return slice
})

var memiterTest = bigslice.Func(func(baseline bigslice.Slice) bigslice.Slice {
	return bigslice.Map(baseline, func(p []byte) []byte {
		q := make([]byte, len(p))
		copy(q, p)
		return q
	})
})

func memiter(sess *exec.Session, args []string) error {
	var (
		flags  = flag.NewFlagSet("memiter", flag.ExitOnError)
		nalloc = flags.Int("n", 128, "number of baseline allocations")
		size   = flags.Int("size", 10<<20, "baseline allocation size")
		niter  = flags.Int("iter", 100, "number of iterations")
	)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: slicer memiter [-n N] [-size S] [-iter I]`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}
	if flags.NArg() != 0 {
		flags.Usage()
	}
	ctx := context.Background()

	result, err := sess.Run(ctx, memiterBaseline, *nalloc, *size)
	if err != nil {
		return err
	}
	for i := 0; i < *niter; i++ {
		if _, err := sess.Run(ctx, memiterTest, result); err != nil {
			return fmt.Errorf("iteration %d: %v", i, err)
		}
		log.Printf("iteration %d: ok", i)
	}
	return nil

	// Gc, heap profile.

}
