// Package bigslicecmd provides the core functionality of the bigslice command
// as a package for easier integration into external toolchains and setups.
// The expected use is that the bigslice run and build commands are common
// to all users of bigslice, but the 'setup-ec2' class of commands are specific.
package bigslicecmd

import (
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"

	"github.com/grailbio/base/fatbin"
	"github.com/grailbio/base/must"
)

var bigsliceMain = template.Must(template.New("bigslice_main.go").Parse(`package main

import (
	"flag"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice/sliceconfig"
	{{.Name}} "{{.ImportPath}}"
)

func main() {
	sess, shutdown := sliceconfig.Parse()
	defer shutdown()
	err := {{.Name}}.BigsliceMain(sess, flag.Args())
	if err != nil {
		log.Fatal(err)
	}
}
`))

// BuildUsage is the usage message for the Build command.
const BuildUsage = `usage: bigslice build [-o output] [inputs]

Command build builds a bigslice binary for the given package or
source files. If no input is given, it is taken to be the package
".".

Build uses the "go" tool to build a fat bigslice binary, consisting
of the native binary for the host GOOS and GOARCH, concatenated with
the binary for GOOS=linux and GOARCH=amd64, since that is the target
platform for Bigslice workers. See package
github.com/grailbio/base/fatbin for more details.

If the host is GOOS=linux, GOARCH=amd64, then the user can use the
regular go tool to build binaries, as the extra build target is not
needed.

The flags are:
`

// Build builds the bigslice binary and writes it out to specified output filename,
// if that string is empty then a suitable name is computed and returned.
func Build(ctx context.Contex, paths []string, output string) (binary string, returnError error) {
	defer func() {
		if r := recover(); r != nil {
			returnErr = fmt.Error("%v", r)
		}
	}()

	must.True(len(paths) > 0, "no paths defined")

	// If we are passed multiple paths, then they must be Go files.
	if len(paths) > 1 {
		for _, path := range paths {
			must.True(filepath.Ext(path) == ".go",
				"multiple paths provided, but ", path, " is not a Go file")
		}
	}

	var info *packageInfo
	if filepath.Ext(paths[0]) != ".go" {
		info = mustLoad(paths[0])
	}
	if output == "" {
		if info == nil {
			output = strings.TrimSuffix(filepath.Base(paths[0]), ".go")
		} else if info.Name == "main" {
			abs, err := filepath.Abs(paths[0])
			must.Nil(err)
			output = filepath.Base(abs)
		} else {
			output = info.Name
		}
	}

	if info == nil {
		fileSet := token.NewFileSet()
		for _, filename := range paths {
			f, err := parser.ParseFile(fileSet, filename, nil, parser.ParseComments)
			if err != nil {
				printGoErrors(err)
				os.Exit(1)
			}
			must.True(f.Name.Name == "main", fileSet.Position(f.Name.NamePos), ": package must be main, not ", f.Name.Name)
		}
	} else if info.Name != "main" {
		var (
			fileSet   = token.NewFileSet()
			foundMain bool
		)
		for _, filename := range info.GoFiles {
			filename = filepath.Join(info.Dir, filename)
			f, err := parser.ParseFile(fileSet, filename, nil, parser.ParseComments)
			if err != nil {
				printGoErrors(err)
				os.Exit(1)
			}
			for _, d := range f.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok {
					continue
				}
				if fn.Recv != nil {
					continue
				}
				if fn.Name.String() != "BigsliceMain" {
					continue
				}
				check := func(v bool) {
					if v {
						return
					}
					pos := fileSet.Position(fn.Pos())
					pos.Filename = shortPath(pos.Filename)
					fmt.Fprintf(os.Stderr, "%s: func BigsliceMain has wrong type; "+
						"expected func(*exec.Session, []string) error\n", pos)
					os.Exit(1)
				}
				check(fn.Type.Results != nil && len(fn.Type.Results.List) == 1 &&
					fn.Type.Params.List != nil &&
					len(fn.Type.Params.List) == 2 &&
					len(fn.Type.Params.List[0].Names) <= 1 &&
					len(fn.Type.Params.List[1].Names) <= 1)

				// Check that the first argument is *Session or *foo.Session.
				// Imports are not resolved here, so we have to check it
				// syntactically.
				ptr, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
				check(ok)
				name, ok := ptr.X.(*ast.Ident)
				asName := ok && name.Name == "Session"
				sel, ok := ptr.X.(*ast.SelectorExpr)
				asSel := ok && sel.Sel.Name == "Session"
				check(asName || asSel)

				slice, ok := fn.Type.Params.List[1].Type.(*ast.ArrayType)
				check(ok)
				name, ok = slice.Elt.(*ast.Ident)
				check(ok && name.Name == "string")

				name, ok = fn.Type.Results.List[0].Type.(*ast.Ident)
				check(ok && name.Name == "error")

				foundMain = true
			}
		}
		if !foundMain {
			fmt.Fprintln(os.Stderr, "func BigsliceMain not found in package", info.ImportPath)
		}

		f, err := ioutil.TempFile("", info.Name+"*.go")
		must.Nil(err)
		must.Nil(bigsliceMain.Execute(f, info))
		paths = []string{f.Name()}
		must.Nil(f.Close())
		defer func() {
			must.Nil(os.Remove(paths[0]))
		}()
	}

	build := exec.Command("go", append([]string{"build", "-o", output}, paths...)...)
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	must.Nil(build.Run())

	// We currently assume that the target is linux/amd64.
	// TODO(marius): don't hard code this.
	if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
		return output, nil
	}

	f, err := ioutil.TempFile("", output)
	must.Nil(err)
	object := f.Name()
	must.Nil(f.Close())

	build = exec.Command("go", append([]string{"build", "-o", object}, paths...)...)
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	build.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")
	must.Nil(build.Run())

	outputFile, err := os.OpenFile(output, os.O_WRONLY|os.O_APPEND, 0777)
	must.Nil(err)
	outputInfo, err := outputFile.Stat()
	must.Nil(err)
	fat := fatbin.NewWriter(outputFile, outputInfo.Size(), runtime.GOOS, runtime.GOARCH)

	linuxAmd64File, err := os.Open(object)
	must.Nil(err)
	w, err := fat.Create("linux", "amd64")
	must.Nil(err)
	_, err = io.Copy(w, linuxAmd64File)
	must.Nil(err)
	must.Nil(os.Remove(object))
	must.Nil(fat.Close())
	must.Nil(linuxAmd64File.Close())
	must.Nil(outputFile.Close())

	return output, nil
}

func printGoErrors(err error) {
	if err, ok := err.(scanner.ErrorList); ok {
		for _, e := range err {
			e.Pos.Filename = shortPath(e.Pos.Filename)
			fmt.Fprintln(os.Stderr, err.Error())
		}
		return
	}
	fmt.Fprintln(os.Stderr, err.Error())
}

func shortPath(path string) string {
	if rel, err := filepath.Rel(cwd, path); err == nil && len(rel) < len(path) {
		return rel
	}
	return path
}
