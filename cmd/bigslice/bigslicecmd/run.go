package bigslicecmd

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

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
	var buildIndex int
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			break
		}
		buildIndex++
	}
	var binary string
	if buildIndex == 0 {
		binary = Build(ctx, []string{"."}, "")
	} else {
		binary = Build(ctx, args[:buildIndex], "")
	}
	if !filepath.IsAbs(binary) {
		// Build may return a relative path that may not include any path
		// separators. If the name passed to CommandContext has no path
		// separators, $PATH is searched instead of using the relative path.
		// Ensure that the name has a path separator.
		binary = "." + string(os.PathSeparator) + binary
	}
	cmd := exec.CommandContext(ctx, binary, args[buildIndex:]...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin
	must.Nil(cmd.Run())
	must.Nil(os.Remove(binary))
}
