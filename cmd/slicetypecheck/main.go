// Standalone bigslice typechecker (alpha).
//
// This is new and in testing. Please report any issues:
// https://github.com/grailbio/bigslice/issues.
//
// TODO: Consider merging this into the main `bigslice` command, when it's well-tested.
// TODO: Consider supporting the golangci-lint plugin interface.
package main

import (
	"github.com/grailbio/bigslice/analysis/typecheck"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(typecheck.Analyzer)
}
