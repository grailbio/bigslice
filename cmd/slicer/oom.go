// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/stress/oom"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
)

var oomTest = bigslice.Func(func(size int) (slice bigslice.Slice) {
	var mu sync.Mutex
	slice = randomReader(100, 1000)
	slice = bigslice.Map(slice, func(key string, values []int) string {
		mu.Lock() // just one passes through per machine
		if size != 0 {
			oom.Do(size)
		}
		oom.Try()
		panic("not reached")
	})
	return slice
})

func oomer(sess *exec.Session, args []string) error {
	var (
		flags = flag.NewFlagSet("oom", flag.ExitOnError)
		size  = flags.Int("size", 0, "size of memory allocation; automatically determined if zero")
	)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: slicer oom [-size bytes]`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	// Currently we only care about how OOMs are reported;
	// we may try to recover from the in the future.
	_, err := sess.Run(ctx, oomTest, *size)
	return err
}
