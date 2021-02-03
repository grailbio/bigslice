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

var Analyzer = &analysis.Analyzer{
	Name: "bigslice_typecheck",
	Doc: `check bigslice func call arguments

Basic typechecker for bigslice programs that inspects session.Run and
session.Must calls to ensure the arguments are compatible with the Func.
Checks are limited by static analysis and are best-effort. For example, the call
	session.Must(ctx, chooseFunc(), args...)
cannot be checked, because it uses chooseFunc() instead of a simple identifier.

Typechecking does not include any slice operations yet.`,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const (
	funcFullName     = "github.com/grailbio/bigslice.Func"
	execMustFullName = "(*github.com/grailbio/bigslice/exec.Session).Must"
	execRunFullName  = "(*github.com/grailbio/bigslice/exec.Session).Run"
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
		for valueIdx, value := range valueSpec.Values {
			call, ok := value.(*ast.CallExpr)
			if !ok {
				continue
			}
			fn := typeutil.StaticCallee(pass.TypesInfo, call)
			if fn == nil {
				continue
			}
			if fn.FullName() != funcFullName {
				continue
			}
			if len(call.Args) != 1 {
				panic(fmt.Errorf("unexpected arguments to bigslice.Func: %v", call.Args))
			}
			implAst := call.Args[0]
			implType := pass.TypesInfo.TypeOf(implAst)
			implSig, ok := implType.(*types.Signature)
			if !ok {
				pass.ReportRangef(implAst, "argument to bigslice.Func must be a function, not %v", implType)
				continue
			}

			var invalidParams bool
			for i := 0; i < implSig.Params().Len(); i++ {
				param := implSig.Params().At(i)
				if err := checkValidFuncArg(param.Type()); err != nil {
					pass.Reportf(param.Pos(),
						"bigslice type error: Func argument %q [%d]: %v", param.Name(), i, err)
					invalidParams = true
				}
			}
			if invalidParams {
				continue
			}

			funcType := pass.TypesInfo.TypeOf(call.Args[0]).Underlying().(*types.Signature)
			funcTypes[valueSpec.Names[valueIdx].Name] = funcType
		}
	})

	inspect.Preorder([]ast.Node{&ast.CallExpr{}}, func(node ast.Node) {
		call := node.(*ast.CallExpr)
		fn := typeutil.StaticCallee(pass.TypesInfo, call)
		if fn == nil {
			return
		}
		if name := fn.FullName(); name != execRunFullName && name != execMustFullName {
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
			pass.ReportRangef(funcValueIdent,
				"bigslice type error: %s requires %d arguments, but got %d",
				funcValueIdent.Name, want, got)
			return
		}

		for i, gotArg := range gotArgs {
			wantType := wantArgTypes.At(i).Type()
			gotType := pass.TypesInfo.TypeOf(gotArg)
			if !types.AssignableTo(gotType, wantType) {
				pass.ReportRangef(gotArg,
					"bigslice type error: func %q argument %q [%d] requires %v, but got %v",
					funcValueIdent.Name, wantArgTypes.At(i).Name(), i, wantType, gotType)
			}
		}
	})

	return nil, nil
}

func checkValidFuncArg(typ types.Type) error {
	switch typ.(type) {
	case *types.Tuple:
		panic("Tuple not expected")
	default:
		// TODO: Consider investigating other types more thoroughly.
		return nil

	case *types.Chan, *types.Signature:
		return fmt.Errorf("unsupported argument type: %s (can't be serialized)", typ.String())
	}
}
