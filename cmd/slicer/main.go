// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Slicer is a binary used to test and stress multiple aspects of Bigslice.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
	"github.com/grailbio/bigslice/sliceconfig"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: slicer [-wait] test-name args...

Command bigslicetest runs large-scale integration testing of various
Bigslice functionality. It's distributed as a separate binary as it
requires launching external clusters, and may run for a long time.

Available test are:

	cogroup
		Large-scale testing of cogroup functionality.
	itermem
		Testing memory leaks during iterative bigslice invocations.
	reduce
		Large-scale testing of reduce functionality.
	oom
		Trigger the OOM killer.
`)
		flag.PrintDefaults()
		os.Exit(2)
	}

	wait := flag.Bool("wait", false, "don't exit after completion")
	sess := sliceconfig.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
	}

	cmd, args := flag.Arg(0), flag.Args()[1:]
	var err error
	switch cmd {
	default:
		fmt.Fprintf(os.Stderr, "unknown command %s\n", cmd)
		flag.Usage()
	case "cogroup":
		err = cogroup(sess, args)
	case "memiter":
		err = memiter(sess, args)
	case "reduce":
		err = reduce(sess, args)
	case "oom":
		err = oomer(sess, args)
	}
	sess.Shutdown()
	if *wait {
		if err != nil {
			log.Printf("finished with error %v: waiting", err)
		} else {
			log.Print("done: waiting")
		}
		<-make(chan struct{})
	}
	must.Nil(err, cmd)
}
