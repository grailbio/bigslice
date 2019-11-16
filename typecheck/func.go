// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package typecheck

import (
	"context"
	"reflect"

	"github.com/grailbio/bigslice/slicetype"
)

var typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()

type funcSliceType struct {
	reflect.Type
}

func (funcSliceType) Prefix() int { return 1 }

// Func deconstructs a function's argument and return types into
// slicetypes. If x is not a function, Func returns false.
//
// If the type of the first argument to the provided function
// is context.Context, then it is ignored: the returned type
// contains only the remainder of the arguments.
func Func(x interface{}) (arg, ret slicetype.Type, ok bool) {
	t := reflect.TypeOf(x)
	if t == nil {
		return nil, nil, false
	}
	if t.Kind() != reflect.Func {
		return nil, nil, false
	}
	in := make([]reflect.Type, t.NumIn())
	for i := range in {
		in[i] = t.In(i)
	}
	if len(in) > 0 && in[0] == typeOfContext {
		in = in[1:]
	}
	return slicetype.New(in...), funcSliceType{t}, true
}
