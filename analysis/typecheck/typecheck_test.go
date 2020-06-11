package typecheck_test

// TODO: Figure out how to test this analyzer.
// The golang.org/x/tools/go/analysis/analysistest testing tools set up a
// "GOPATH-style project" [1], which seems to require vendoring dependencies.
// This is ok for testing the Go standard library but isn't practical for
// code with several transitive dependencies, as bigslice has.
// [1] https://pkg.go.dev/golang.org/x/tools@v0.0.0-20200611032120-1fdcbd130028/go/analysis/analysistest?tab=doc
//
// Until then, run and see no errors for urls but a type error for wrongarg:
// 	$ go run github.com/grailbio/bigslice/cmd/slicetypecheck \
// 		github.com/grailbio/bigslice/cmd/urls \
// 		github.com/grailbio/bigslice/analysis/typecheck/typechecktest/wrongarg
//	<snip>/wrongarg.go:22:34: bigslice type error: func "testFunc" argument "argInt" requires int, but got string
//	exit status 3
