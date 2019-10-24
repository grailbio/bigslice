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

	"github.com/grailbio/base/must"
	"github.com/grailbio/bigslice/cmd/bigslice/bigslicecmd"
)

func buildCmdUsage(flags *flag.FlagSet) {
	fmt.Fprint(os.Stderr, bigslicecmd.BuildUsage)
	flags.PrintDefaults()
	os.Exit(2)
}

func buildCmd(args []string) {
	var (
		flags  = flag.NewFlagSet("bigslice build", flag.ExitOnError)
		output = flags.String("o", "", "output path")
	)
	flags.Usage = func() { buildCmdUsage(flags) }
	must.Nil(flags.Parse(args))

	if len(flags.Args()) == 0 {
		log.Fatal("no arguments")
	}

	paths := flags.Args()
	if len(paths) == 0 {
		paths = []string{"."}
	}
	ctx := context.Background()
	bigslicecmd.Build(ctx, paths, *output)
}
