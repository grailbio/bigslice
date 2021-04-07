// Tests the static slice typechecker.
//
// This is a correctly-typed Go program (even though it's incomplete and thus
// not runnable, for simplicity). However, its bigslice Func invocation is
// incorrectly typed, and the static typechecker find that, in this case.
package main

import (
	"context"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
)

var testFunc = bigslice.Func(func(argInt int, argString string) bigslice.Slice {
	return nil
})

func main() {
	ctx := context.Background()
	var session *exec.Session
	_ = session.Must(ctx, testFunc, "i should be an int", "i'm ok")
}
