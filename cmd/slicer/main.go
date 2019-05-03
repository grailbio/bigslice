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
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/slicecmd"
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
`)
		flag.PrintDefaults()
		os.Exit(2)
	}

	wait := flag.Bool("wait", false, "don't exit after completion")
	slicecmd.Main(func(sess *exec.Session, args []string) error {
		if len(args) == 0 {
			flag.Usage()
		}
		cmd, args := args[0], args[1:]
		var err error
		switch cmd {
		default:
			fmt.Fprintf(os.Stderr, "unknown command %s\n", cmd)
			flag.Usage()
		case "cogroup":
			err = cogroup(sess, args)
		case "memiter":
			err = memiter(sess, args)
		}
		if *wait {
			if err != nil {
				log.Printf("finished with error %v: waiting", err)
			} else {
				log.Print("done: waiting")
			}
			<-make(chan struct{})
		}
		return err
	})
}
