// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/grailbio/bigslice/cmd/bigslice/bigslicecmd"
)

func runCmdUsage() {
	fmt.Fprintf(os.Stderr, bigslicecmd.RunUsage)
	os.Exit(2)
}

func runCmd(args []string) {
	var buildIndex int
	for _, arg := range args {
		if arg == "-help" || arg == "--help" {
			runCmdUsage()
		}
		if strings.HasPrefix(arg, "-") {
			break
		}
		buildIndex++
	}
	bigslicecmd.Run(context.Background(), args[buildIndex:])
}
