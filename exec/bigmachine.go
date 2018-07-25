// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"bufio"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/ctxsync"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/stats"
	"golang.org/x/sync/errgroup"
)

func init() {
	gob.Register(invocationRef{})
}

const (
	// StatsPollInterval is the period at which task statistics are polled.
	statsPollInterval = 10 * time.Second

	// StatTimeout is the maximum amount of time allowed to retrieve
	// machine stats, per iteration.
	statTimeout = 5 * time.Second
)

// RetryPolicy is the default retry policy used for machine calls.
var retryPolicy = retry.Backoff(time.Second, 5*time.Second, 1.5)

// FatalErr is used to match fatal errors.
var fatalErr = errors.E(errors.Fatal)

// DoShuffleReaders determines whether reader tasks should be
// shuffled in order to avoid potential thundering herd issues.
// This should only be used in testing when deterministic ordering
// matters.
//
// TODO(marius): make this a session option instead.
var DoShuffleReaders = true

func init() {
	gob.Register(&worker{})
}

// TODO(marius): clean up flag registration, etc. vis-a-vis bigmachine.
// e.g., perhaps we can register flags in a bigmachine flagset that gets
// parsed together, so that we don't litter the process with global flags.

// BigmachineExecutor is an executor that runs individual tasks on
// bigmachine machines.
type bigmachineExecutor struct {
	system bigmachine.System

	sess *Session
	b    *bigmachine.B

	offerc chan *sliceMachine
	needc  chan int

	status *status.Group

	mu sync.Mutex

	locations map[*Task]*sliceMachine
	stats     map[string]stats.Values

	// Invocations and invocationDeps are used to track dependencies
	// between invocations so that we can execute arbitrary graphs of
	// slices on bigmachine workers. Note that this requires that we
	// hold on to the invocations, which is somewhat unfortunate, but
	// I don't see a clean way around it.
	invocations    map[uint64]bigslice.Invocation
	invocationDeps map[uint64]map[uint64]bool
}

func newBigmachineExecutor(system bigmachine.System) *bigmachineExecutor {
	return &bigmachineExecutor{system: system}
}

// Start starts registers the bigslice worker with bigmachine and then
// starts the bigmachine.
//
// TODO(marius): provide fine-grained fault tolerance.
func (b *bigmachineExecutor) Start(sess *Session) (shutdown func()) {
	b.sess = sess
	b.b = bigmachine.Start(b.system)
	b.locations = make(map[*Task]*sliceMachine)
	b.stats = make(map[string]stats.Values)
	if status := sess.Status(); status != nil {
		b.status = status.Group("bigmachine")
	}
	b.invocations = make(map[uint64]bigslice.Invocation)
	b.invocationDeps = make(map[uint64]map[uint64]bool)
	b.offerc = make(chan *sliceMachine)
	b.needc = make(chan int)
	go manageMachines(context.Background(), b.b, b.status, b.sess.Parallelism(), b.sess.MaxLoad(), b.needc, b.offerc)
	return b.b.Shutdown
}

func (b *bigmachineExecutor) Runnable(task *Task) {
	task.Lock()
	switch task.state {
	case TaskWaiting, TaskRunning:
		task.Unlock()
		return
	}
	task.state = TaskWaiting
	task.Broadcast()
	task.Unlock()
	go b.run(task)
}

type invocationRef struct{ Index uint64 }

func (b *bigmachineExecutor) compile(ctx context.Context, m *sliceMachine, inv bigslice.Invocation) error {
	// Substitute Result arguments for an invocation ref and record the
	// dependency.
	b.mu.Lock()
	for i, arg := range inv.Args {
		result, ok := arg.(*Result)
		if !ok {
			continue
		}
		inv.Args[i] = invocationRef{result.inv.Index}
		if _, ok := b.invocations[result.inv.Index]; !ok {
			b.mu.Unlock()
			return fmt.Errorf("invalid result invocation %x", result.inv.Index)
		}
		if b.invocationDeps[inv.Index] == nil {
			b.invocationDeps[inv.Index] = make(map[uint64]bool)
		}
		b.invocationDeps[inv.Index][result.inv.Index] = true
	}
	b.invocations[inv.Index] = inv

	// Now traverse the invocation graph bottom-up, making sure
	// everything on the machine is compiled. We produce a valid order,
	// but we don't capture opportunities for parallel compilations.
	// This seems needless for most uses.
	var (
		todo        = []uint64{inv.Index}
		invocations []bigslice.Invocation
	)
	for len(todo) > 0 {
		var i uint64
		i, todo = todo[0], todo[1:]
		invocations = append(invocations, b.invocations[i])
		for j := range b.invocationDeps[i] {
			todo = append(todo, j)
		}
	}
	b.mu.Unlock()

	for i := len(invocations) - 1; i >= 0; i-- {
		err := m.Compiles.Do(invocations[i].Index, func() error {
			return m.RetryCall(ctx, "Worker.Compile", invocations[i], nil)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *bigmachineExecutor) commit(ctx context.Context, m *sliceMachine, key string) error {
	return m.Commits.Do(key, func() error {
		log.Printf("committing key %v on worker %v", key, m.Addr)
		return m.RetryCall(ctx, "Worker.CommitCombiner", key, nil)
	})
}

func (b *bigmachineExecutor) run(task *Task) {
	ctx := context.Background()
	task.Status.Print("waiting for a machine")

	b.need(1)
	defer b.need(-1)

	var m *sliceMachine
	select {
	case <-ctx.Done():
		task.Error(ctx.Err())
		return
	case m = <-b.offerc:
	}

	numTasks := m.Stats.Int("tasks")
	numTasks.Add(1)
	m.UpdateStatus()
	defer func() {
		numTasks.Add(-1)
		m.UpdateStatus()
	}()

	// Make sure that the invocation has been compiled on the selected
	// machine.
compile:
	for {
		err := b.compile(ctx, m, task.Invocation)
		switch {
		case err == nil:
			break compile
		case ctx.Err() == nil && (err == context.Canceled || err == context.DeadlineExceeded):
			// In this case, we've caught a context error from a prior
			// invocation. We're going to try to run it again. Note that this
			// is racy: the behavior remains correct but may imply additional
			// data transfer. C'est la vie.
			m.Compiles.Forget(task.Invocation.Index)
		case errors.Is(errors.Net, err), errors.IsTemporary(err):
			// Compilations don't involve invoking user code, nor does it
			// involve dependencies other than potentially uploading data from
			// the driver node, so we interpret errors more strictly.
			task.Status.Printf("task lost while compiling bigslice.Func: %v", err)
			task.Set(TaskLost)
			m.Done(err)
			return
		default:
			task.Errorf("failed to compile invocation on machine %s: %v", m.Addr, err)
			m.Done(err)
			return
		}
	}

	// Populate the run request. Include the locations of all dependent
	// outputs so that the receiving worker can read from them.
	req := taskRunRequest{
		Task:       task.Name,
		Invocation: task.Invocation.Index,
		Locations:  make(map[string]string),
	}
	g, _ := errgroup.WithContext(ctx)
	for _, dep := range task.Deps {
		for _, deptask := range dep.Tasks {
			m := b.location(deptask)
			if m == nil {
				// TODO(marius): make this a separate state, or a separate
				// error type?
				task.Errorf("task %s has no location", deptask.Name)
				m.Done(nil)
				return
			}
			req.Locations[deptask.Name] = m.Addr
			key := dep.CombineKey
			if key == "" {
				continue
			}
			// Make sure that the result is committed.
			g.Go(func() error { return b.commit(ctx, m, key) })
		}
	}
	task.Status.Print(m.Addr)
	if err := g.Wait(); err != nil {
		task.Errorf("failed to commit combiner: %v", err)
		return
	}

	// While we're running, also update task stats directly into the tasks's status.
	// TODO(marius): also aggregate stats across all tasks.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(statsPollInterval):
			}
			var vals stats.Values
			if err := m.Call(ctx, "Worker.Stats", struct{}{}, &vals); err != nil {
				if err != context.Canceled {
					log.Error.Printf("Worker.Stats: %v", err)
				}
				return
			}
			task.Status.Printf("%s: %s", m.Addr, vals)
			b.mu.Lock()
			name := fmt.Sprintf("%s(%x)", task.Name, task.Invocation.Index)
			b.stats[name] = vals
			b.mu.Unlock()
			b.updateStatus()
		}
	}()

	task.Set(TaskRunning)
	var reply taskRunReply
	err := m.RetryCall(ctx, "Worker.Run", req, &reply)
	m.Done(err)
	switch {
	case err == nil:
		b.setLocation(task, m)
		task.Set(TaskOk)
		m.Assign(task)
	case ctx.Err() != nil:
		task.Error(err)
	case errors.Match(fatalErr, err):
		// Fatal errors aren't retryable.
		task.Error(err)
	default:
		// Everything else we consider as the task being lost. It'll get
		// resubmitted by the evaluator.
		task.Status.Printf("lost task during task evaluation: %v", err)
		task.Set(TaskLost)
	}
}

func (b *bigmachineExecutor) Reader(ctx context.Context, task *Task, partition int) sliceio.Reader {
	m := b.location(task)
	if m == nil {
		return sliceio.ErrReader(errors.E(errors.NotExist, fmt.Sprintf("task %s", task.Name)))
	}
	if task.CombineKey != "" {
		return sliceio.ErrReader(fmt.Errorf("read %s: cannot read tasks with combine keys", task.Name))
	}
	// TODO(marius): access the store here, too, in case it's a shared one (e.g., s3)
	return &machineReader{
		Machine:       m.Machine,
		TaskPartition: taskPartition{task.Name, partition},
	}
}

// Need indicates the n additional procs are needed.
func (b *bigmachineExecutor) need(n int) {
	b.needc <- n
}

func (b *bigmachineExecutor) HandleDebug(handler *http.ServeMux) {
	b.b.HandleDebug(handler)
}

// Location returns the machine on which the results of the provided
// task resides.
func (b *bigmachineExecutor) location(task *Task) *sliceMachine {
	b.mu.Lock()
	m := b.locations[task]
	b.mu.Unlock()
	return m
}

func (b *bigmachineExecutor) setLocation(task *Task, m *sliceMachine) {
	b.mu.Lock()
	b.locations[task] = m
	b.mu.Unlock()
}

func (b *bigmachineExecutor) updateStatus() {
	total := make(stats.Values)
	b.mu.Lock()
	for _, stat := range b.stats {
		for k, v := range stat {
			total[k] += v
		}
	}
	b.mu.Unlock()
	b.status.Print(total)
}

type combinerState int

const (
	combinerNone combinerState = iota
	combinerWriting
	combinerCommitted
	combinerError
	combinerIdle
	// States > combinerIdle are reference counts.
)

// A worker is the bigmachine service that runs individual tasks and serves
// the results of previous runs. Currently all output is buffered in memory.
type worker struct {
	// Exported just satisfies gob's persnickety nature: we need at least
	// one exported field.
	Exported struct{}

	b     *bigmachine.B
	store Store

	mu       sync.Mutex
	cond     *ctxsync.Cond
	compiles taskOnce
	tasks    map[uint64]map[string]*Task
	slices   map[uint64]bigslice.Slice
	stats    *stats.Map

	// CombinerStates and combiners are used to manage shared combine
	// buffers.
	combinerStates map[string]combinerState
	combiners      map[string][]chan *combiner

	commitLimiter *limiter.Limiter
}

func (w *worker) Init(b *bigmachine.B) error {
	w.cond = ctxsync.NewCond(&w.mu)
	w.tasks = make(map[uint64]map[string]*Task)
	w.slices = make(map[uint64]bigslice.Slice)
	w.combiners = make(map[string][]chan *combiner)
	w.combinerStates = make(map[string]combinerState)
	w.b = b
	dir, err := ioutil.TempDir("", "bigslice")
	if err != nil {
		return err
	}
	w.store = &fileStore{Prefix: dir + "/"}
	w.stats = stats.NewMap()
	// Set up a limiter to limit the number of concurrent commits
	// that are allowed to happen in the worker.
	//
	// TODO(marius): we should treat commits like tasks and apply
	// load balancing/limiting instead.
	w.commitLimiter = limiter.New()
	procs := b.System().Maxprocs()
	if procs == 0 {
		procs = runtime.GOMAXPROCS(0)
	}
	w.commitLimiter.Release(procs)
	return nil
}

// Compile compiles an invocation on the worker and stores the
// resulting tasks. Compile is idempotent: it will compile each
// invocation at most once.
func (w *worker) Compile(ctx context.Context, inv bigslice.Invocation, _ *struct{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("invocation panic! %v", e)
			err = errors.E(errors.Fatal, err)
		}
	}()
	return w.compiles.Do(inv.Index, func() error {
		// Substitute invocation refs for the results of the invocation.
		// The executor must ensure that all references have been compiled.
		for i, arg := range inv.Args {
			ref, ok := arg.(invocationRef)
			if !ok {
				continue
			}
			w.mu.Lock()
			inv.Args[i], ok = w.slices[ref.Index]
			w.mu.Unlock()
			if !ok {
				return fmt.Errorf("worker.Compile: invalid invocation reference %x", ref.Index)
			}
		}
		slice := inv.Invoke()
		tasks, err := compile(make(taskNamer), inv, slice)
		if err != nil {
			return err
		}
		all := make(map[*Task]bool)
		for _, task := range tasks {
			task.all(all)
		}
		named := make(map[string]*Task)
		for task := range all {
			named[task.Name] = task
		}
		w.mu.Lock()
		w.tasks[inv.Index] = named
		w.slices[inv.Index] = &Result{Slice: slice, tasks: tasks}
		w.mu.Unlock()
		return nil
	})
}

// TaskRunRequest contains all data required to run an individual task.
type taskRunRequest struct {
	// Invocation is the invocation from which the task was compiled.
	Invocation uint64

	// Task is the name of the task compiled from Invocation.
	Task string

	// Locations contains the locations of the output of each dependency.
	Locations map[string]string
}

type taskRunReply struct{} // nothing here yet

// Run runs an individual task as described in the request. Run
// returns a nil error when the task was successfully run and its
// output deposited in a local buffer.
func (w *worker) Run(ctx context.Context, req taskRunRequest, reply *taskRunReply) (err error) {
	recordsOut := w.stats.Int("write")
	named := w.tasks[req.Invocation]
	if named == nil {
		return errors.E(errors.Fatal, fmt.Errorf("invocation %x not compiled", req.Invocation))
	}
	task := named[req.Task]
	if task == nil {
		return errors.E(errors.Fatal, fmt.Errorf("task %s not found", req.Task))
	}

	task.Lock()
	if task.state != TaskInit {
		for task.state <= TaskRunning {
			log.Printf("runtask: %s already running. Waiting for it to finish.", task.Name)
			err = task.Wait(ctx)
			if err != nil {
				break
			}
		}
		task.Unlock()
		if e := task.Err(); e != nil {
			err = e
		}
		return err
	}
	task.state = TaskRunning
	task.Unlock()
	defer func() {
		if e := recover(); e != nil {
			stack := debug.Stack()
			err = fmt.Errorf("panic while evaluating slice: %v\n%s", e, string(stack))
			err = errors.E(err, errors.Fatal)
		}
		if err != nil {
			log.Printf("task %s error: %v", req.Task, err)
			task.Error(errors.Recover(err))
		} else {
			task.Set(TaskOk)
		}
	}()

	// Gather inputs from the bigmachine cluster, dialing machines
	// as necessary.
	var (
		totalRecordsIn *stats.Int
		recordsIn      *stats.Int
	)
	if len(task.Deps) > 0 {
		totalRecordsIn = w.stats.Int("inrecords")
		recordsIn = w.stats.Int("read")
	}
	in := make([]sliceio.Reader, 0, len(task.Deps))
	for _, dep := range task.Deps {
		// If the dependency has a combine key, they are combined on the
		// machine, and we de-dup the dependencies.
		//
		// The caller of has already ensured that the combiner buffers
		// are committed on the machines.
		if dep.CombineKey != "" {
			locations := make(map[string]bool)
			for _, deptask := range dep.Tasks {
				addr := req.Locations[deptask.Name]
				if addr == "" {
					return fmt.Errorf("no location for input task %s", deptask.Name)
				}
				// We only read the first combine key for each location.
				//
				// TODO(marius): compute some non-overlapping intersection of
				// combine keys instead, so that we can handle error recovery
				// properly. In particular, in the case of error recovery, we
				// have to create new combiner keys so that they aren't written
				// into previous combiner buffers. This suggests that combiner
				// keys should be assigned by the executor, and not during
				// compile time.
				if locations[addr] {
					continue
				}
				locations[addr] = true
			}
			for addr := range locations {
				machine, err := w.b.Dial(ctx, addr)
				if err != nil {
					return err
				}
				r := &machineReader{
					Machine:       machine,
					TaskPartition: taskPartition{dep.CombineKey, dep.Partition},
				}
				in = append(in, &statsReader{r, recordsIn})
				defer r.Close()
			}
		} else {
			reader := new(multiReader)
			reader.q = make([]sliceio.Reader, len(dep.Tasks))
			// We shuffle the tasks here so that we don't encounter "thundering herd"
			// issues were partitions are read sequentially from the same (ordered)
			// list of machines.
			//
			// TODO(marius): possibly we should perform proper load balancing here
			shuffled := rand.Perm(len(dep.Tasks))

		Tasks:
			for j := range dep.Tasks {
				k := j
				if DoShuffleReaders {
					k = shuffled[j]
				}
				deptask := dep.Tasks[k]
				// If we have it locally, or if we're using a shared backend store
				// (e.g., S3), then read it directly.
				info, err := w.store.Stat(ctx, deptask.Name, dep.Partition)
				if err == nil {
					rc, err := w.store.Open(ctx, deptask.Name, dep.Partition, 0)
					if err == nil {
						defer rc.Close()
						reader.q[j] = sliceio.NewDecodingReader(rc)
						totalRecordsIn.Add(info.Records)
						continue Tasks
					}
				}
				// Find the location of the task.
				addr := req.Locations[deptask.Name]
				if addr == "" {
					return fmt.Errorf("no location for input task %s", deptask.Name)
				}
				machine, err := w.b.Dial(ctx, addr)
				if err != nil {
					return err
				}
				tp := taskPartition{deptask.Name, dep.Partition}
				if err := machine.Call(ctx, "Worker.Stat", tp, &info); err != nil {
					return err
				}
				r := &machineReader{
					Machine:       machine,
					TaskPartition: tp,
				}
				reader.q[j] = &statsReader{r, recordsIn}
				totalRecordsIn.Add(info.Records)
				defer r.Close()
			}
			if dep.Expand {
				in = append(in, reader.q...)
			} else {
				in = append(in, reader)
			}
		}
	}

	// If we have a combiner, then we partition globally for the machine
	// into common combiners.
	if task.Combiner != nil {
		return w.runCombine(ctx, task, task.Do(in))
	}

	// Stream partition output directly to the underlying store, but
	// through a buffer because the column encoder can make small
	// writes.
	//
	// TODO(marius): switch to using a monotasks-like arrangement
	// instead once we also have memory management, in order to control
	// buffer growth.
	type partition struct {
		wc  writeCommitter
		buf *bufio.Writer
		*sliceio.Encoder
	}
	partitions := make([]*partition, task.NumPartition)
	for p := range partitions {
		wc, err := w.store.Create(ctx, task.Name, p)
		if err != nil {
			return err
		}
		// TODO(marius): pool the writers so we can reuse them.
		part := new(partition)
		part.wc = wc
		part.buf = bufio.NewWriter(wc)
		part.Encoder = sliceio.NewEncoder(part.buf)
		partitions[p] = part
	}
	defer func() {
		for p, part := range partitions {
			if part == nil {
				continue
			}
			if err := part.wc.Discard(ctx); err != nil {
				log.Printf("discard %s partition %d: %v", task.Name, p, err)
			}
		}
	}()
	out := task.Do(in)
	count := make([]int64, task.NumPartition)
	switch {
	case task.NumOut() == 0:
		// If there are no output columns, just drive the computation.
		_, err := out.Read(ctx, frame.Empty)
		if err == sliceio.EOF {
			err = nil
		}
		return err
	case task.NumPartition > 1:
		const psize = defaultChunksize / 100
		var (
			partitionv = make([]frame.Frame, task.NumPartition)
			lens       = make([]int, task.NumPartition)
		)
		for i := range partitionv {
			partitionv[i] = frame.Make(task, psize, psize)
		}
		in := frame.Make(task, defaultChunksize, defaultChunksize)
		for {
			n, err := out.Read(ctx, in)
			if err != nil && err != sliceio.EOF {
				return err
			}
			for i := 0; i < n; i++ {
				p := int(in.Hash(i)) % task.NumPartition
				j := lens[p]
				frame.Copy(partitionv[p].Slice(j, j+1), in.Slice(i, i+1))
				lens[p]++
				count[p]++
				// Flush when we fill up.
				if lens[p] == psize {
					if err := partitions[p].Encode(partitionv[p]); err != nil {
						return err
					}
					partitionv[p].Clear()
					lens[p] = 0
				}
			}
			recordsOut.Add(int64(n))
			if err == sliceio.EOF {
				break
			}
		}
		// Flush remaining data.
		for p, n := range lens {
			if n == 0 {
				continue
			}
			if err := partitions[p].Encode(partitionv[p].Slice(0, n)); err != nil {
				return err
			}
		}
	default:
		in := frame.Make(task, defaultChunksize, defaultChunksize)
		for {
			n, err := out.Read(ctx, in)
			if err != nil && err != sliceio.EOF {
				return err
			}
			if err := partitions[0].Encode(in.Slice(0, n)); err != nil {
				return err
			}
			recordsOut.Add(int64(n))
			count[0] += int64(n)
			if err == sliceio.EOF {
				break
			}
		}
	}

	for i, part := range partitions {
		if err := part.buf.Flush(); err != nil {
			return err
		}
		partitions[i] = nil
		if err := part.wc.Commit(ctx, count[i]); err != nil {
			return err
		}
	}
	partitions = nil
	return nil
}

func (w *worker) runCombine(ctx context.Context, task *Task, in sliceio.Reader) error {
	w.mu.Lock()
	switch w.combinerStates[task.CombineKey] {
	case combinerWriting, combinerCommitted, combinerError:
		w.mu.Unlock()
		return fmt.Errorf("combine key %s already committed", task.CombineKey)
	case combinerNone:
		combiners := make([]chan *combiner, task.NumPartition)
		for i := range combiners {
			comb, err := newCombiner(task, fmt.Sprintf("%s%d", task.CombineKey, i), *task.Combiner, defaultChunksize*100)
			if err != nil {
				w.mu.Unlock()
				for j := 0; j < i; j++ {
					if err := (<-combiners[j]).Discard(); err != nil {
						log.Error.Printf("error discarding combiner: %v", err)
					}
				}
				return err
			}
			combiners[i] = make(chan *combiner, 1)
			combiners[i] <- comb
		}
		w.combiners[task.CombineKey] = combiners
		w.combinerStates[task.CombineKey] = combinerIdle
	}
	w.combinerStates[task.CombineKey]++
	combiners := w.combiners[task.CombineKey]
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.combinerStates[task.CombineKey]--
		w.mu.Unlock()
	}()

	recordsOut := w.stats.Int("write")
	// Now perform the partition-combine operation. We maintain a
	// per-task combine buffer for each partition. When this buffer
	// reaches half of its capacity, we attempt to combine up to 3/4ths
	// of its least frequent keys into the shared combine buffer. This
	// arrangement permits hot keys to be combined primarily in the task
	// buffer, while spilling less frequent keys into the per machine
	// buffer. (The local buffer is purely in memory, and has a fixed
	// capacity; the machine buffer spills to disk when it reaches a
	// preconfigured threshold.)
	var (
		partitionCombiner = make([]*combiningFrame, task.NumPartition)
		out               = frame.Make(task, defaultChunksize, defaultChunksize)
	)
	for i := range partitionCombiner {
		partitionCombiner[i] = makeCombiningFrame(task, *task.Combiner, 8, 1)
	}
	for {
		n, err := in.Read(ctx, out)
		if err != nil && err != sliceio.EOF {
			return err
		}
		for i := 0; i < n; i++ {
			p := int(out.Hash(i)) % task.NumPartition
			pcomb := partitionCombiner[p]
			pcomb.Combine(out.Slice(i, i+1))

			len, cap := pcomb.Len(), pcomb.Cap()
			if len <= cap/2 {
				continue
			}
			var combiner *combiner
			if len >= 8 {
				combiner = <-combiners[p]
			} else {
				select {
				case combiner = <-combiners[p]:
				default:
					continue
				}
			}

			flushed := pcomb.Compact()
			err := combiner.Combine(ctx, flushed)
			combiners[p] <- combiner
			if err != nil {
				return err
			}
		}
		recordsOut.Add(int64(n))
		if err == sliceio.EOF {
			break
		}
	}
	// Flush the remainder.
	for p, comb := range partitionCombiner {
		combiner := <-combiners[p]
		err := combiner.Combine(ctx, comb.Compact())
		combiners[p] <- combiner
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *worker) Stats(ctx context.Context, _ struct{}, values *stats.Values) error {
	w.stats.AddAll(*values)
	return nil
}

// TaskPartition names a partition of a task.
type taskPartition struct {
	// Task is the name of the task whose output is to be read.
	Task string
	// Partition is the partition number to read.
	Partition int
}

// Stat returns the SliceInfo for a slice.
func (w *worker) Stat(ctx context.Context, tp taskPartition, info *sliceInfo) (err error) {
	*info, err = w.store.Stat(ctx, tp.Task, tp.Partition)
	return
}

// CommitCombiner commits the current combiner buffer with the
// provided key. After successful return, its results are available via
// Read.
func (w *worker) CommitCombiner(ctx context.Context, key string, _ *struct{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		switch w.combinerStates[key] {
		case combinerNone:
			return fmt.Errorf("invalid combiner key %s", key)
		case combinerWriting:
			if err := w.cond.Wait(ctx); err != nil {
				return err
			}
		case combinerCommitted:
			return nil
		case combinerError:
			return errors.E("error while writing combiner")
		case combinerIdle:
			w.combinerStates[key] = combinerWriting
			go w.writeCombiner(key)
		default:
			return fmt.Errorf("combiner key %s busy", key)
		}
	}
}

func (w *worker) writeCombiner(key string) {
	ctx := context.Background()
	g, ctx := errgroup.WithContext(context.Background())
	for part := range w.combiners[key] {
		part, combiner := part, <-w.combiners[key][part]
		g.Go(func() error {
			w.commitLimiter.Acquire(ctx, 1)
			defer w.commitLimiter.Release(1)
			wc, err := w.store.Create(ctx, key, part)
			if err != nil {
				return err
			}
			buf := bufio.NewWriter(wc)
			enc := sliceio.NewEncoder(buf)
			n, err := combiner.WriteTo(ctx, enc)
			if err != nil {
				wc.Discard(ctx)
				return err
			}
			if err := buf.Flush(); err != nil {
				wc.Discard(ctx)
				return err
			}
			return wc.Commit(ctx, n)
		})
	}
	err := g.Wait()
	w.mu.Lock()
	w.combiners[key] = nil
	if err == nil {
		w.combinerStates[key] = combinerCommitted
	} else {
		log.Error.Printf("failed to write combine buffer %s: %v", key, err)
		w.combinerStates[key] = combinerError
	}
	w.cond.Broadcast()
	w.mu.Unlock()
}

// Read reads a slice.
//
// TODO(marius): should we flush combined outputs explicitly?
func (w *worker) Read(ctx context.Context, req readRequest, rc *io.ReadCloser) (err error) {
	*rc, err = w.store.Open(ctx, req.Task, req.Partition, req.Offset)
	return
}

type machineRPCReader struct {
	ctx context.Context
	// Machine is the machine from which task data is read.
	machine *bigmachine.Machine
	// TaskPartition is the task and partition that should be read.
	taskPartition taskPartition
	err           error
	reader        io.ReadCloser // The raw data from the remote worker
	bytes         int64         // Cumulative # of bytes read from the worker.
	retries       int
}

// readRequest is the request payload for Worker.Run
type readRequest struct {
	// Task is the name of the task whose output is to be read.
	Task string
	// Partition is the partition number to read.
	Partition int
	// Offset is the start offset of the read
	Offset int64
}

func (r *machineRPCReader) Read(data []byte) (int, error) {
	for {
		if r.err != nil {
			return 0, r.err
		}
		if r.reader == nil {
			if r.retries > 0 {
				log.Printf("Worker.Read %s: retrying(%d) rpc from offset %d",
					r.taskPartition.Task, r.retries, r.bytes)
			}
			if err := r.machine.RetryCall(r.ctx, "Worker.Read",
				readRequest{r.taskPartition.Task, r.taskPartition.Partition, r.bytes}, &r.reader); err != nil {
				// machine.Call retries on temp errors, so we don't need to retry here.
				r.err = err
				return 0, r.err
			}
		}
		n, err := r.reader.Read(data)
		if err == nil || err == io.EOF {
			r.err = err
			r.bytes += int64(n)
			return n, err
		}
		// Here, we blindly retry regardless of error kind/severity.
		// This allows us to retry on on errors such as aws-sdk or io.UnexpectedEOF.
		// The subsequent call to Worker.Read will detect any permenent
		// errors in any case.
		log.Error.Printf("machineReader %s: error (%d) at %d bytes: %v",
			r.machine.Addr, r.retries, r.bytes, err)
		r.reader.Close()
		r.reader = nil
		r.retries++
		if r.err = retry.Wait(r.ctx, retryPolicy, r.retries); r.err != nil {
			return 0, r.err
		}
	}
}

func (r *machineRPCReader) Close() error {
	if r.reader == nil {
		return nil
	}
	err := r.reader.Close()
	r.reader = nil
	return err
}

// MachineReader reads a taskPartition from a machine. It issues the
// (streaming) read RPC on the first call to Read so that data are
// not buffered unnecessarily. MachineReaders close themselves after
// they have been read to completion; they should otherwise be closed
// if they are not read to completion.
type machineReader struct {
	// Machine is the machine from which task data is read.
	Machine *bigmachine.Machine
	// TaskPartition is the task and partition that should be read.
	TaskPartition taskPartition

	reader sliceio.Reader
	rpc    *machineRPCReader
}

func newMachineReader(machine *bigmachine.Machine, partition taskPartition) *machineReader {
	m := &machineReader{
		Machine:       machine,
		TaskPartition: partition,
	}
	return m
}

func (m *machineReader) Read(ctx context.Context, f frame.Frame) (int, error) {
	if m.rpc == nil {
		m.rpc = &machineRPCReader{
			ctx:           ctx,
			machine:       m.Machine,
			taskPartition: m.TaskPartition,
		}
		m.reader = sliceio.NewDecodingReader(m.rpc)
	}
	n, err := m.reader.Read(ctx, f)
	return n, err
}

func (m *machineReader) Close() error {
	if m.rpc != nil {
		return m.rpc.Close()
	}
	return nil
}

type statsReader struct {
	reader  sliceio.Reader
	numRead *stats.Int
}

func (s *statsReader) Read(ctx context.Context, f frame.Frame) (n int, err error) {
	n, err = s.reader.Read(ctx, f)
	s.numRead.Add(int64(n))
	return
}
