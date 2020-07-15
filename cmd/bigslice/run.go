// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/grailbio/bigslice/cmd/bigslice/bigslicecmd"
)

func runCmdUsage() {
	fmt.Fprintf(os.Stderr, bigslicecmd.RunUsage)
	os.Exit(2)
}

func runCmd(args []string) {
	if len(args) == 0 {
		runCmdUsage()
	}
	if args[0] == "-help" || args[0] == "--help" {
		runCmdUsage()
	}
	bigslicecmd.Run(context.Background(), args)
}
