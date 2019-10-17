package bigslicecmd

import (
	"context"
	"os"
	"os/exec"

	"github.com/grailbio/base/must"
)

// RunUsage is the usage message for the Run command.
const RunUsage = `usage: bigslice run [input] [flags]

Command run builds and then runs the provided package or files. See
"bigslice build -help" for more details.
`

// Run executes the supplied arguments as a subprocess. If no arguments are
// supplied, Build(ctx, ".") is used to build the current package and run that.
func Run(ctx context.Context, args []string) {
	var binary string
	if len(args) == 0 {
		binary = Build(ctx, []string{"."}, "")
	} else {
		binary, args = args[0], args[1:]
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin
	must.Nil(cmd.Run())
	must.Nil(os.Remove(binary))
	return
}
