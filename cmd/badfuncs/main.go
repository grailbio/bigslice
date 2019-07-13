// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Badfuncs is a binary that tests various scenarios of Func creation that may
// fail to satisfy the invariant that all workers share common definitions of
// Funcs. All tests should result in a panic except for 'ok'.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/slicecmd"
)

var makeFuncs = []func() *bigslice.FuncValue{
	func() *bigslice.FuncValue {
		return bigslice.Func(func() bigslice.Slice {
			return bigslice.Const(4, []int{})
		})
	},
	func() *bigslice.FuncValue {
		return bigslice.Func(func() bigslice.Slice {
			return bigslice.Const(4, []int{})
		})
	},
	func() *bigslice.FuncValue {
		return bigslice.Func(func() bigslice.Slice {
			return bigslice.Const(4, []int{})
		})
	},
	func() *bigslice.FuncValue {
		return bigslice.Func(func() bigslice.Slice {
			return bigslice.Const(4, []int{})
		})
	},
}

func ok() {
	funcs := make([]*bigslice.FuncValue, len(makeFuncs))
	for i, makeFunc := range makeFuncs {
		funcs[i] = makeFunc()
	}
	slicecmd.Main(func(sess *exec.Session, args []string) error {
		ctx := context.Background()
		_, err := sess.Run(ctx, funcs[0])
		if err != nil {
			return err
		}
		return nil
	})
}

func toolate() {
	slicecmd.Main(func(sess *exec.Session, args []string) error {
		f0 := makeFuncs[0]()
		ctx := context.Background()
		_, err := sess.Run(ctx, f0)
		if err != nil {
			return err
		}
		return nil
	})
}

func random() {
	rand.Seed(time.Now().UTC().UnixNano())
	rand.Shuffle(len(makeFuncs), func(i, j int) {
		makeFuncs[i], makeFuncs[j] = makeFuncs[j], makeFuncs[i]
	})
	funcs := make([]*bigslice.FuncValue, len(makeFuncs))
	for i, makeFunc := range makeFuncs {
		funcs[i] = makeFunc()
	}
	slicecmd.Main(func(sess *exec.Session, args []string) error {
		ctx := context.Background()
		_, err := sess.Run(ctx, funcs[0])
		if err != nil {
			return err
		}
		return nil
	})
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: badfuncs [slicecmd-options] test-name

Command badfuncs tests various scenarios of Func creation that may fail to
satisfy the invariant that all workers share common definitions of Funcs. All
tests should result in a panic except for 'ok'.

Available tests are:

	ok
		Funcs are properly created.
	toolate
		Funcs are created after exec.Start, so they are not available on
		workers.
	random
		Funcs are created in random order. (Note that this may not panic if all
		workers randomly produce the same funcs).

`)
		flag.PrintDefaults()
		os.Exit(2)
	}
	if len(os.Args) < 2 {
		flag.Usage()
	}
	cmd := os.Args[len(os.Args)-1]
	switch cmd {
	case "ok":
		ok()
	case "toolate":
		toolate()
	case "random":
		random()
	default:
		fmt.Fprintf(os.Stderr, "unknown test %s\n", cmd)
		flag.Usage()
	}
}
