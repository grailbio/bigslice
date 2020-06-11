package typecheck

import (
	"fmt"
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/types/typeutil"
)

const Doc = `check bigslice exec call arguments

Basic typechecker for bigslice programs that inspects session.Run and
session.Must calls to ensure the arguments are compatible with the Func.
Checks are limited by static analysis and are best-effort. For example, the call
	session.Must(ctx, chooseFunc(), args...)
cannot be checked, because it uses chooseFunc() instead of a simple identifier.

Typechecking does not include any slice operations yet.`

var Analyzer = &analysis.Analyzer{
	Name:     "bigslice_typecheck",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const (
	bigslicePkgPath     = "github.com/grailbio/bigslice"
	execPkgPath         = "github.com/grailbio/bigslice/exec"
	funcValueTypeString = "*github.com/grailbio/bigslice.FuncValue"
)

func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// funcTypes describes the types of declared bigslice.FuncValues.
	// TODO: As stated below, we're only recording top-level func for now.
	// If that changes, this map should be keyed by a global identifier, not *ast.Ident.
	// TODO: We may also want to return these types as Facts to allow checking across packages.
	funcTypes := map[string]*types.Signature{}

	// Collect the types of bigslice.Funcs.
	// TODO: ValueSpec captures top-level vars, but maybe we should include non-top-level ones, too.
	inspect.Preorder([]ast.Node{&ast.ValueSpec{}}, func(node ast.Node) {
		valueSpec := node.(*ast.ValueSpec)
		for i := range valueSpec.Values {
			// valueType := pass.TypesInfo.TypeOf(valueSpec.Values[i])
			// if valueType.String() != funcValueTypeString {
			// 	continue
			// }
			call, ok := valueSpec.Values[i].(*ast.CallExpr)
			if !ok {
				continue
			}
			fn := typeutil.StaticCallee(pass.TypesInfo, call)
			if fn == nil {
				continue
			}
			if fn.Pkg().Path() != bigslicePkgPath || fn.Name() != "Func" {
				continue
			}
			if len(call.Args) != 1 {
				panic(fmt.Errorf("unexpected arguments to bigslice.Func: %v", call.Args))
			}
			funcType := pass.TypesInfo.TypeOf(call.Args[0]).Underlying().(*types.Signature)
			funcTypes[valueSpec.Names[i].Name] = funcType
		}
	})

	inspect.Preorder([]ast.Node{&ast.CallExpr{}}, func(node ast.Node) {
		call := node.(*ast.CallExpr)
		fn := typeutil.StaticCallee(pass.TypesInfo, call)
		if fn == nil {
			return
		}
		if fn.Pkg().Path() != execPkgPath || (fn.Name() != "Must" && fn.Name() != "Run") {
			return
		}

		funcValueIdent, ok := call.Args[1].(*ast.Ident)
		if !ok {
			// This function invocation is more complicated than a simple identifier.
			// Give up on typechecking this call.
			return
		}
		funcType, ok := funcTypes[funcValueIdent.Name]
		if !ok {
			// TODO: Propagate bigslice.Func types as Facts so we can do a better job here.
			return
		}

		wantArgTypes := funcType.Params()
		gotArgs := call.Args[2:]
		if want, got := wantArgTypes.Len(), len(gotArgs); want != got {
			pass.Report(analysis.Diagnostic{
				Pos: funcValueIdent.Pos(),
				End: funcValueIdent.End(),
				Message: fmt.Sprintf(
					"bigslice type error: %s requires %d arguments, but got %d",
					funcValueIdent.Name, want, got),
			})
			return
		}

		for i, gotArg := range gotArgs {
			wantType := wantArgTypes.At(i).Type()
			gotType := pass.TypesInfo.TypeOf(gotArg)
			if !types.AssignableTo(gotType, wantType) {
				pass.Report(analysis.Diagnostic{
					Pos: gotArg.Pos(),
					End: gotArg.End(),
					Message: fmt.Sprintf(
						"bigslice type error: func %q argument %q requires %v, but got %v",
						funcValueIdent.Name, wantArgTypes.At(i).Name(), wantType, gotType),
				})
			}
		}
	})

	return nil, nil
}
