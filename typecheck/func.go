// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package typecheck

import (
	"reflect"

	"github.com/grailbio/bigslice/slicetype"
)

// Func deconstructs a function's argument and return types into
// slicetypes. If x is not a function, Func returns false.
func Func(x interface{}) (arg, ret slicetype.Type, ok bool) {
	t := reflect.TypeOf(x)
	if t == nil {
		return nil, nil, false
	}
	if t.Kind() != reflect.Func {
		return nil, nil, false
	}
	in := make([]reflect.Type, t.NumIn())
	for i := 0; i < t.NumIn(); i++ {
		in[i] = t.In(i)
	}
	return slicetype.New(in...), t, true
}
