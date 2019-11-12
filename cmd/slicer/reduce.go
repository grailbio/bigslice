// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
)

var reduceTest = bigslice.Func(func(nshard, nkey int) (slice bigslice.Slice) {
	log.Printf("reduceTest(%d, %d)", nshard, nkey)
	slice = randomReader(nshard, nkey)
	slice = bigslice.Map(slice, func(key string, xs []int) (string, int) { return key, len(xs) })
	slice = bigslice.Reduce(slice, func(a int, e int) int {
		return a + e
	})
	return
})

func reduce(sess *exec.Session, args []string) error {
	var (
		flags  = flag.NewFlagSet("cogroup", flag.ExitOnError)
		nshard = flags.Int("nshard", 64, "number of shards")
		nkey   = flags.Int("nkey", 1e6, "number of keys per shard")
	)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: slicer reduce [-nshard N] [-nkey N]`)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err := flags.Parse(args); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	r, err := sess.Run(ctx, reduceTest, *nshard, *nkey)
	if err != nil {
		return err
	}
	scan := r.Scanner()
	defer scan.Close()
	ok := true
	errorf := func(format string, v ...interface{}) {
		log.Error.Printf(format, v...)
		ok = false
	}
	var (
		keystr string
		count  int
		seen   = make([]bool, *nkey)
	)
	for scan.Scan(ctx, &keystr, &count) {
		key, err := strconv.Atoi(keystr)
		if err != nil {
			panic(err)
		}
		if seen[key] {
			errorf("saw key %v multiple times", key)
		}
		seen[key] = true
		if count != *nshard {
			errorf("wrong value for key %s: %v", key, count)
		}
	}
	if err := scan.Err(); err != nil {
		return err
	}
	for key, saw := range seen {
		if !saw {
			errorf("did not see key %v", key)
		}
	}
	if !ok {
		return errors.New("test errors")
	}
	fmt.Println("ok")
	return nil
}
