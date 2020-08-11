// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package typecheck

import (
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/slicetype"
)

// CanApply returns whether fn can be applied to arg.
func CanApply(fn slicefunc.Func, arg slicetype.Type) bool {
	if fn.IsVariadic {
		if arg.NumOut() < fn.In.NumOut()-1 {
			// Not enough arguments.
			return false
		}
		for i := 0; i < fn.In.NumOut()-1; i++ {
			if !arg.Out(i).AssignableTo(fn.In.Out(i)) {
				// Non-variadic mismatch.
				return false
			}
		}
		variadicType := fn.In.Out(fn.In.NumOut() - 1).Elem()
		for i := fn.In.NumOut() - 1; i < arg.NumOut(); i++ {
			if !arg.Out(i).AssignableTo(variadicType) {
				// Variadic mismatch.
				return false
			}
		}
		return true
	}
	if arg.NumOut() != fn.In.NumOut() {
		return false
	}
	for i := 0; i < fn.In.NumOut(); i++ {
		if !arg.Out(i).AssignableTo(fn.In.Out(i)) {
			return false
		}
	}
	return true
}
