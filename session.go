// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"fmt"
	"reflect"
)

// Session represents a Bigslice compute session. A session shares a
// binary and executor, and is valid for the run of the binary. A
// session can run multiple bigslice functions, allowing for
// iterative computing.
//
// A session is started by the Start method. Some executors use
// may launch multiple copies of the binary: these additional binaries
// are called workers and Start in these Start does not return.
//
// All functions must be created before Start is called, and must be
// created in a deterministic order. This is provided by default when
// functions are created as part of package initialization. Registering
// toplevel functions this way is both safe and encouraged:
//
//	var Computation = bigslice.Func(func(..) (slice Slice) {
//		// Build up the computation, parameterized by the function.
//		slice = ...
//		slice = ...
//		return slice
//	})
//
//	// Possibly in another package:
//	func main() {
//		sess := bigslice.Start()
//		if err := sess.Run(ctx, Computation, args...); err != nil {
//			log.Fatal(err)
//		}
//		// Success!
//	}
type Session struct {
	context.Context
	executor Executor
	tasks    map[uint64][]*Task
	types    map[uint64][]reflect.Type
}

// An Option represents a session configuration parameter value.
type Option func(s *Session)

var (
	// Local configures a session with the local in-binary executor.
	Local Option = func(s *Session) {
		s.executor = newLocalExecutor()
	}
	// Bigmachine configures a session with the bigmachine executor.
	// This is default.
	Bigmachine Option = func(s *Session) {
		s.executor = newBigmachineExecutor()
	}
)

// Start creates and starts a new bigslice session, configuring it
// according to the provided options. Only one session may be created
// in a single binary invocation. The returned session remains valid for
// the lifetime of the binary. If no executor is configured, the session
// is configured to use the bigmachine executor.
func Start(options ...Option) *Session {
	s := &Session{
		Context: context.Background(),
		tasks:   make(map[uint64][]*Task),
		types:   make(map[uint64][]reflect.Type),
	}
	for _, opt := range options {
		opt(s)
	}
	if s.executor == nil {
		s.executor = newBigmachineExecutor()
	}
	s.executor.Start(s.Context)
	return s
}

// Run evaluates the slice returned by the bigslice func funcv
// applied to the provided arguments. Tasks are run by the session's
// executor. Run returns when the computation has completed, or else
// on error. It is not safe to make concurrent calls to Run. Instead,
// parallelism should be expressed in the bigslice computation
// itself.
func (s *Session) Run(ctx context.Context, funcv *FuncValue, args ...interface{}) error {
	// TODO(marius): perform structural equality checking too, and panic
	// if there's a collision, or maybe add a counter to name colliding
	// applications.
	inv := funcv.Invocation(args...)
	key := inv.Sum64()
	tasks, ok := s.tasks[key]
	if !ok {
		slice := funcv.Apply(args...)
		// All task names are prefixed with the invocation key.
		namer := newTaskNamer(fmt.Sprintf("%d/", key))
		var err error
		tasks, err = compile(namer, slice)
		if err != nil {
			return err
		}
		s.tasks[key] = tasks
		s.types[key] = ColumnTypes(slice)
	}
	return Eval(ctx, s.executor, inv, tasks)
}
