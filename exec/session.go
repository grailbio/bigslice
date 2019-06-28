// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"encoding/gob"
	"fmt"
	"net/http"
	"runtime"
	"sync"

	"github.com/grailbio/base/backgroundcontext"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
)

// DefaultMaxLoad is the default machine max load.
const DefaultMaxLoad = 0.95

func init() {
	gob.Register(&Result{})
}

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
//		sess := exec.Start()
//		if err := sess.Run(ctx, Computation, args...); err != nil {
//			log.Fatal(err)
//		}
//		// Success!
//	}
type Session struct {
	context.Context
	shutdown func()
	p        int
	maxLoad  float64
	executor Executor
	status   *status.Status

	machineCombiners bool

	tracer *tracer

	mu sync.Mutex
	// roots stores all task roots compiled by this session;
	// used for debugging.
	roots map[*Task]struct{}
}

// An Option represents a session configuration parameter value.
type Option func(s *Session)

// Local configures a session with the local in-binary executor.
var Local Option = func(s *Session) {
	s.executor = newLocalExecutor()
}

// Bigmachine configures a session using the bigmachine executor
// configured with the provided system. If any params are provided,
// they are applied to each bigmachine allocated by Bigslice.
func Bigmachine(system bigmachine.System, params ...bigmachine.Param) Option {
	return func(s *Session) {
		s.executor = newBigmachineExecutor(system, params...)
	}
}

// Parallelism configures the session with the provided target
// parallelism.
func Parallelism(p int) Option {
	if p <= 0 {
		panic("exec.Parallelism: p <= 0")
	}
	return func(s *Session) {
		s.p = p
	}
}

// MaxLoad configures the session with the provided max
// machine load.
func MaxLoad(maxLoad float64) Option {
	if maxLoad <= 0 {
		panic("exec.MaxLoad: maxLoad <= 0")
	}
	return func(s *Session) {
		s.maxLoad = maxLoad
	}
}

// Status configures the session with a status object to which
// run statuses are reported.
func Status(status *status.Status) Option {
	return func(s *Session) {
		s.status = status
	}
}

// MachineCombiners is a session option that turns on machine-local
// combine buffers. If turned on, each combiner task that belongs to
// the same shard-set and runs on the same machine combines values
// into a single, machine-local combine buffer. This can be a big
// performance optimization for tasks that have low key cardinality,
// or a key-set with very hot keys. However, due to the way it is
// implemented, error recovery is currently not implemented for such
// tasks.
var MachineCombiners Option = func(s *Session) {
	s.machineCombiners = true
}

// Start creates and starts a new bigslice session, configuring it
// according to the provided options. Only one session may be created
// in a single binary invocation. The returned session remains valid for
// the lifetime of the binary. If no executor is configured, the session
// is configured to use the bigmachine executor.
func Start(options ...Option) *Session {
	s := &Session{
		Context: backgroundcontext.Get(),
		roots:   make(map[*Task]struct{}),
	}
	for _, opt := range options {
		opt(s)
	}
	if s.p == 0 {
		s.p = 1
	}
	if s.maxLoad == 0 {
		s.maxLoad = DefaultMaxLoad
	}
	if s.executor == nil {
		s.executor = newBigmachineExecutor(bigmachine.Local)
	}
	s.shutdown = s.executor.Start(s)
	s.tracer = newTracer()
	return s
}

// Run evaluates the slice returned by the bigslice func funcv
// applied to the provided arguments. Tasks are run by the session's
// executor. Run returns when the computation has completed, or else
// on error. It is safe to make concurrent calls to Run; the
// underlying computation will be performed in parallel.
func (s *Session) Run(ctx context.Context, funcv *bigslice.FuncValue, args ...interface{}) (*Result, error) {
	return s.run(ctx, 1, funcv, args...)
}

// Must is a version of Run that panics if the computation fails.
func (s *Session) Must(ctx context.Context, funcv *bigslice.FuncValue, args ...interface{}) *Result {
	res, err := s.run(ctx, 1, funcv, args...)
	if err != nil {
		log.Panicf("exec.Run: %v", err)
	}
	return res
}

func (s *Session) run(ctx context.Context, calldepth int, funcv *bigslice.FuncValue, args ...interface{}) (*Result, error) {
	location := "<unknown>"
	if _, file, line, ok := runtime.Caller(calldepth + 1); ok {
		location = fmt.Sprintf("%s:%d", file, line)
	}
	inv := funcv.Invocation(location, args...)
	slice := inv.Invoke()
	tasks, _, err := compile(make(taskNamer), inv, slice, s.machineCombiners)
	if err != nil {
		return nil, err
	}
	// TODO(marius): give a way to provide names for these groups
	var group *status.Group
	if s.status != nil {
		group = s.status.Groupf("run %s [%d]", location, inv.Index)
	}
	// Register all the tasks so they may be used in visualization.
	s.mu.Lock()
	for _, task := range tasks {
		s.roots[task] = struct{}{}
	}
	s.mu.Unlock()
	return &Result{
		Slice: slice,
		sess:  s,
		inv:   inv,
		tasks: tasks,
	}, Eval(ctx, s.executor, inv, tasks, group)
}

// Parallelism returns the desired amount of evaluation parallelism.
func (s *Session) Parallelism() int {
	return s.p
}

// MaxLoad returns the maximum load on each allocated machine.
func (s *Session) MaxLoad() float64 {
	return s.maxLoad
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
	handler.Handle("/debug", http.HandlerFunc(s.handleDebug))
	handler.Handle("/debug/tasks/graph", http.HandlerFunc(s.handleTasksGraph))
	handler.Handle("/debug/tasks", http.HandlerFunc(s.handleTasks))
	if s.tracer != nil {
		handler.HandleFunc("/debug/trace", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("content-type", "application/json; charset=utf-8")
			if err := s.tracer.Marshal(w); err != nil {
				log.Error.Printf("exec.Session: /debug/trace: marshal: %v", err)
			}
		})
	}
}

// A Result is the output of a Slice evaluation. It is the only type
// implementing bigslice.Slice that is a legal argument to a
// bigslice.Func.
type Result struct {
	bigslice.Slice
	inv   bigslice.Invocation
	sess  *Session
	tasks []*Task
}

// Scan returns a scanner that scans the output. If the output
// contains multiple shards, they are scanned sequentially.
func (r *Result) Scan(ctx context.Context) *sliceio.Scanner {
	readers := make([]sliceio.Reader, len(r.tasks))
	for i := range readers {
		readers[i] = r.sess.executor.Reader(ctx, r.tasks[i], 0)
	}
	return &sliceio.Scanner{
		Type:   r,
		Reader: sliceio.MultiReader(readers...),
	}
}
