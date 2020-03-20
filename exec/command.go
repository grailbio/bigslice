// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"os"
	"strings"
)

// shellQuote quotes a string to be used as an argument in an sh command line.
func shellQuote(s string) string {
	// We wrap with single quotes, as they will work with any string except
	// those with single quotes. We handle single quotes by tranforming them
	// into "'\''" and letting the shell concatenate the strings back together.
	return "'" + strings.Replace(s, "'", `'\''`, -1) + "'"
}

// command returns the command-line of the current execution. The format can be
// directly pasted into sh to be run.
func command() string {
	args := make([]string, len(os.Args))
	for i := range args {
		args[i] = shellQuote(os.Args[i])
	}
	return strings.Join(args, " ")
}
