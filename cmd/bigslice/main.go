// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
	"github.com/grailbio/bigslice/cmd/bigslice/bigslicecmd"
)

/*
	bigslice run -local ...
	bigslice run -cluster=ec2 github.com/grailbio/blah -args
	bigslice run -cluster=ec2 -- -foo -bar
	bigslice list // configured clusters, where they are coming from?
*/

func usage() {
	fmt.Fprintf(os.Stderr, `Bigslice is a tool for managing Bigslice builds and configuration.

Usage:

	bigslice <command> [arguments]

The commands are:

	setup-ec2   configure EC2 for use with Bigslice
	build       build a bigslice program
	run         run a bigslice program or source files
`)
	// TODO(marius): this command pulls in way too many global flags
	// from other modules, including Vanadium; these dependencies
	// should be pruned.
	os.Exit(2)
}

func main() {
	log.AddFlags()
	log.SetFlags(0)
	log.SetPrefix("bigslice: ")
	must.Func = func(depth int, v ...interface{}) {
		// Nothing to do if output fails.
		_ = log.Output(depth+1, log.Error, fmt.Sprint(v...))
		os.Exit(1)
	}
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
	}

	if err := bigslicecmd.Init(); err != nil {
		log.Fatal(err)
	}

	cmd, args := flag.Arg(0), flag.Args()[1:]
	switch cmd {
	default:
		fmt.Fprintln(os.Stderr, "unknown command", cmd)
		flag.Usage()
	case "run":
		runCmd(args)
	case "build":
		buildCmd(args)
	case "setup-ec2":
		setupEc2Cmd(args)
	}
}
