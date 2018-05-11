// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"net/http"
	"reflect"

	"github.com/grailbio/base/status"
	"github.com/grailbio/bigmachine"
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
	shutdown func()
	p        int
	executor Executor
	tasks    map[uint64][]*Task
	types    map[uint64][]reflect.Type
	status   *status.Status
}

// An Option represents a session configuration parameter value.
type Option func(s *Session)

// Local configures a session with the local in-binary executor.
var Local Option = func(s *Session) {
	s.executor = newLocalExecutor()
}

// Bigmachine configures a session using the bigmachine executor
// configured with the provided system.
func Bigmachine(system bigmachine.System) Option {
	return func(s *Session) {
		s.executor = newBigmachineExecutor(system)
	}
}

// Parallelism configures the session with the provided target
// parallelism.
func Parallelism(p int) Option {
	return func(s *Session) {
		s.p = p
	}
}

// Status configures the session with a status object to which
// run statuses are reported.
func Status(status *status.Status) Option {
	return func(s *Session) {
		s.status = status
	}
}

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
	if s.p == 0 {
		s.p = 1
	}
	if s.executor == nil {
		s.executor = newBigmachineExecutor(bigmachine.Local)
	}
	s.shutdown = s.executor.Start(s)
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
	key := inv.Index
	tasks, ok := s.tasks[key]
	if !ok {
		slice := inv.Invoke()
		// All task names are prefixed with the invocation key.
		var err error
		tasks, err = compile(make(taskNamer), inv, slice)
		if err != nil {
			return err
		}
		s.tasks[key] = tasks
		s.types[key] = ColumnTypes(slice)
	}
	// TODO(marius): give a way to provide names for these groups
	var group *status.Group
	if s.status != nil {
		group = s.status.Group("bigslice")
	}
	return Eval(ctx, s.executor, inv, tasks, group)
}

// Parallelism returns the desired amount of evaluation parallelism.
func (s *Session) Parallelism() int {
	return s.p
}

// Shutdown tears down resources associated with this session.
// It should be called when the session is discarded.
func (s *Session) Shutdown() {
	if s.shutdown != nil {
		s.shutdown()
	}
}

// Status returns the session's status aggregator.
func (s *Session) Status() *status.Status {
	return s.status
}

func (s *Session) HandleDebug(handler *http.ServeMux) {
	s.executor.HandleDebug(http.DefaultServeMux)
}
