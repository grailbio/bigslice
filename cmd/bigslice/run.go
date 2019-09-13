// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/grailbio/base/must"
)

func runCmdUsage() {
	fmt.Fprintf(os.Stderr, `usage: bigslice run [input] [flags]

Command run builds and then runs the provided package or files. See
"bigslice build -help" for more details.
`)
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
	var binary string
	if buildIndex == 0 {
		binary = build([]string{"."}, "")
	} else {
		binary = build(args[:buildIndex], "")
	}

	cmd := exec.Command(binary, args[buildIndex:]...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin
	must.Nil(cmd.Run())
	must.Nil(os.Remove(binary))
}
