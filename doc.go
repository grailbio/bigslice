// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// TODO(marius): fill this in some more, especially once tooling
// improves.

/*
	Package bigslice implements a distributed data processing system.
	Users compose computations by operating over large collections ("big
	slices") of data, transforming them with a handful of combinators.
	While users express computations using collections-style operations,
	bigslice takes care of the details of parallel execution and
	distribution across multiple machines.

	Bigslice jobs can run locally, but uses bigmachine for distribution
	among a cluster of compute nodes. In either case, user code does not
	change; the details of distribution are handled by the combination
	of bigmachine and bigslice.

	Because Go cannot easily serialize code to be sent over the wire and
	executed remotely, bigslice programs have to be written with a few
	constraints:

	1. All slices must be constructed by bigslice funcs (bigslice.Func), and all
	such functions must be instantiated before bigslice.Start is called. This
	rule is easy to follow: if funcs are global variables, and bigslice.Start
	is called from a program's main, then the program is compliant.

	2. The driver program must be compiled on the same GOOS and GOARCH as the
	target architecture. When running locally, this is not a concern, but
	programs that require distribution must be run from a linux/amd64 binary.
	Bigslice also supports the fat binary format implemented by
	github.com/grailbio/base/fatbin. The bigslice tool
	(github.com/grailbio/bigslice/cmd/bigslice) uses this package to compile
	portable fat binaries.

	Some Bigslice operations may be annotated with runtime pragmas: directives
	for the Bigslice runtime. See Pragma for details.

	User provided functions in Bigslice

	Functions provided to the various bigslice combinators (e.g., bigslice.Map)
	may take an additional argument of type context.Context. If specified, then
	the lifetime of the context is tied to that of the underlying bigslice task.
	Additionally, the context carries a metrics scope
	(github.com/grailbio/base/bigslice/metrics.Scope) which can be used to update
	metric values during data processing.

*/
package bigslice
