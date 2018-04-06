// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"runtime/debug"
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/bigmachine"
	"golang.org/x/sync/errgroup"
)

func init() {
	gob.Register(&worker{})
}

// TODO(marius): clean up flag registration, etc. vis-a-vis bigmachine.
// e.g., perhaps we can register flags in a bigmachine flagset that gets
// parsed together, so that we don't litter the process with global flags.

// LoadedMachine maintains a load for each machine in order
// to load balance tasks among them.
type loadedMachine struct {
	*bigmachine.Machine
	Load int
}

type machineHeap []loadedMachine

func (h machineHeap) Len() int           { return len(h) }
func (h machineHeap) Less(i, j int) bool { return h[i].Load < h[j].Load }
func (h machineHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *machineHeap) Push(x interface{}) {
	*h = append(*h, x.(loadedMachine))
}

func (h *machineHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// BigmachineExecutor is an executor that runs individual tasks on
// bigmachine machines.
type bigmachineExecutor struct {
	system bigmachine.System

	sess *Session
	b    *bigmachine.B

	machines     []*bigmachine.Machine
	machinesOnce sync.Once
	machinesErr  error

	mu        sync.Mutex
	load      machineHeap
	locations map[*Task]*bigmachine.Machine
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
	b.locations = make(map[*Task]*bigmachine.Machine)
	heap.Init(&b.load)
	for _, m := range b.machines {
		heap.Push(&b.load, loadedMachine{m, 0})
	}
	return b.b.Shutdown
}

// Run runs the task (of the provided invocation) on the least-loaded
// bigmachine. Bigmachine workers arrange for output to be streamed
// directly from the machines on which they were deposited.
//
// TODO(marius): provide a standardized means of propagating
// status updates and logs.
func (b *bigmachineExecutor) Run(ctx context.Context, inv Invocation, task *Task) (err error) {
	if err := b.initMachines(); err != nil {
		return err
	}
	b.mu.Lock()
	m := heap.Pop(&b.load).(loadedMachine)
	m.Load++
	heap.Push(&b.load, m)
	b.mu.Unlock()

	// Populate the run request. Include the locations of all dependent
	// outputs so that the receiving worker can read from them.
	req := taskRunRequest{
		Task:       task.Name,
		Invocation: inv,
		Locations:  make(map[string]string),
	}
	for _, dep := range task.Deps {
		for _, deptask := range dep.Tasks {
			m := b.location(deptask)
			if m == nil {
				return fmt.Errorf("task %s has no location", deptask.Name)
			}
			req.Locations[deptask.Name] = m.Addr
		}
	}
	task.Status.Print(m.Addr)
	var reply taskRunReply
	err = m.Call(ctx, "Worker.Run", req, &reply)
	b.mu.Lock()
	for i := range b.load {
		if b.load[i] == m {
			b.load[i].Load--
			heap.Fix(&b.load, i)
			break
		}
	}
	b.mu.Unlock()
	if err == nil {
		b.setLocation(task, m.Machine)
	}
	return err
}

func (b *bigmachineExecutor) Reader(ctx context.Context, task *Task, partition int) Reader {
	m := b.location(task)
	if m == nil {
		return errorReader{errors.E(errors.NotExist, fmt.Sprintf("task %s", task.Name))}
	}
	// TODO(marius): access the store here, too, in case it's a shared one (e.g., s3)
	req := taskReadRequest{
		Task:      task.Name,
		Partition: partition,
	}
	var rc io.ReadCloser
	if err := m.Call(ctx, "Worker.Read", req, &rc); err != nil {
		return errorReader{err}
	}
	return &closingReader{
		Reader: newDecodingReader(rc),
		Closer: rc,
	}
}

// Maxprocs reports the total number of processors available in
// the bigmachine.
func (b *bigmachineExecutor) Maxprocs() int {
	// TODO(marius): cache this
	var n int
	for _, mach := range b.machines {
		n += mach.Maxprocs
	}
	return n
}

func (b *bigmachineExecutor) initMachines() error {
	b.machinesOnce.Do(func() {
		var (
			n        = 1
			p        = b.sess.Parallelism()
			maxprocs = b.b.System().Maxprocs()
		)
		if p > 0 {
			n = p / maxprocs
			if p%maxprocs != 0 {
				n++
			}
		}
		log.Printf("starting %d bigmachines (p=%d, maxprocs=%d)", n, p, maxprocs)
		ctx := context.Background()
		machines, err := b.b.StartN(ctx, n, bigmachine.Services{
			"Worker": &worker{},
		})
		if err != nil {
			b.machinesErr = err
			return
		}
		log.Printf("waiting for %d machines", len(machines))
		g, ctx := errgroup.WithContext(ctx)
		for i := range machines {
			m := machines[i]
			g.Go(func() error {
				<-m.Wait(bigmachine.Running)
				if err := m.Err(); err != nil {
					log.Printf("machine %s failed to start: %v", m.Addr, err)
					return nil
				}
				log.Printf("machine %v is ready", m.Addr)
				b.mu.Lock()
				b.machines = append(b.machines, m)
				b.mu.Unlock()
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			b.machinesErr = err
			return
		}
		if len(b.machines) == 0 {
			b.machinesErr = errors.E("no machines started")
			return
		}
		b.mu.Lock()
		for _, m := range b.machines {
			heap.Push(&b.load, loadedMachine{m, 0})
		}
		b.mu.Unlock()
	})
	return b.machinesErr
}

func (b *bigmachineExecutor) HandleDebug(handler *http.ServeMux) {
	b.b.HandleDebug(handler)
}

// Location returns the machine on which the results of the provided
// task resides.
func (b *bigmachineExecutor) location(task *Task) *bigmachine.Machine {
	b.mu.Lock()
	m := b.locations[task]
	b.mu.Unlock()
	return m
}

func (b *bigmachineExecutor) setLocation(task *Task, m *bigmachine.Machine) {
	b.mu.Lock()
	b.locations[task] = m
	b.mu.Unlock()
}

// A worker is the bigmachine service that runs individual tasks and serves
// the results of previous runs. Currently all output is buffered in memory.
type worker struct {
	// Exported just satisfies gob's persnickety nature: we need at least
	// one exported field.
	Exported struct{}

	b *bigmachine.B

	mu          sync.Mutex
	store       Store
	invocations map[uint64]bool
	tasks       map[string]*Task
}

func (w *worker) Init(b *bigmachine.B) error {
	w.tasks = make(map[string]*Task)
	w.invocations = make(map[uint64]bool)
	w.b = b
	dir, err := ioutil.TempDir("", "bigslice")
	if err != nil {
		return err
	}
	w.store = &fileStore{Prefix: dir + "/"}
	return nil
}

// TaskRunRequest contains all data required to run an individual task.
type taskRunRequest struct {
	// Task is the name of the task to be run.
	Task string

	// Invocation is the invocation from which the task was produced.
	Invocation Invocation

	// Locations contains the locations of the output of each dependency.
	Locations map[string]string
}

type taskRunReply struct{} // nothing here yet

// Run runs an individual task as described in the request. Run
// returns a nil error when the task was successfully run and its
// output deposited in a local buffer.
func (w *worker) Run(ctx context.Context, req taskRunRequest, reply *taskRunReply) (err error) {
	defer func() {
		if e := recover(); e != nil {
			stack := debug.Stack()
			err = fmt.Errorf("panic while evaluating slice: %v\n%s", e, string(stack))
		}
		if err != nil {
			log.Printf("task %s error: %v", req.Task, err)
		}
	}()

	// Perform an invocation if needed.
	w.mu.Lock()
	if key := req.Invocation.Sum64(); !w.invocations[key] {
		slice := req.Invocation.Invoke()
		namer := newTaskNamer(fmt.Sprintf("%d/", key))
		tasks, err := compile(namer, slice)
		if err != nil {
			w.mu.Unlock()
			return err
		}
		all := make(map[*Task]bool)
		for _, task := range tasks {
			task.all(all)
		}
		for task := range all {
			w.tasks[task.Name] = task
		}
		w.invocations[key] = true
	}
	task := w.tasks[req.Task]
	w.mu.Unlock()

	if task == nil {
		return fmt.Errorf("task %s is not registered", req.Task)
	}
	// Gather inputs from the bigmachine cluster, dialing machines
	// as necessary.
	in := make([]Reader, len(task.Deps))
	for i, dep := range task.Deps {
		reader := new(multiReader)
		reader.q = make([]Reader, len(dep.Tasks))
		for j, deptask := range dep.Tasks {
			// If we have it locally, or if we're using a shared backend store
			// (e.g., S3), then read it directly.
			rc, err := w.store.Open(ctx, deptask.Name, dep.Partition)
			if err == nil {
				defer rc.Close()
				reader.q[j] = newDecodingReader(rc)
				continue
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
			req := taskReadRequest{
				Task:      deptask.Name,
				Partition: dep.Partition,
			}
			if err := machine.Call(ctx, "Worker.Read", req, &rc); err != nil {
				return err
			}
			reader.q[j] = newDecodingReader(rc)
			defer rc.Close()
		}
		in[i] = reader
	}

	// Stream partition output directly to the underlying store, but
	// through a buffer because the column encoder can make small
	// writes.
	//
	// TODO(marius): switch to using a monotasks-like arrangement
	// instead once we also have memory management, in order to control
	// buffer growth.
	type partition struct {
		closer io.Closer
		buf    *bufio.Writer
		*Encoder
	}
	partitions := make([]*partition, task.NumPartition)
	for p := range partitions {
		wc, err := w.store.Create(ctx, task.Name, p)
		if err != nil {
			return err
		}
		// TODO(marius): pool the writers so we can reuse them.
		part := new(partition)
		part.closer = wc
		part.buf = bufio.NewWriter(wc)
		part.Encoder = NewEncoder(part.buf)
		partitions[p] = part
	}
	defer func() {
		for p, part := range partitions {
			if err := part.closer.Close(); err != nil {
				log.Printf("close %s partition %d: %v", task.Name, p, err)
			}
		}
	}()
	out := task.Do(in)
	switch {
	case len(task.Out) == 0:
		// If there are no output columns, just drive the computation.
		_, err := out.Read(ctx)
		if err == EOF {
			err = nil
		}
		return err
	case task.NumPartition == 1:
		in := makeOutputVectors(task)
		for {
			n, err := out.Read(ctx, in...)
			if err != nil && err != EOF {
				return err
			}
			limit(in, n)
			if err := partitions[0].Encode(in...); err != nil {
				return err
			}
			if err == EOF {
				break
			}
			unlimit(in)
		}
	default:
		var (
			partition  = make([]int, defaultChunksize)
			partitionv = make([][]reflect.Value, task.NumPartition)
			lens       = make([]int, task.NumPartition)
		)
		for i := range partitionv {
			partitionv[i] = makeOutputVectors(task)
		}
		in := makeOutputVectors(task)
		for {
			n, err := out.Read(ctx, in...)
			if err != nil && err != EOF {
				return err
			}
			task.Partitioner.Partition(in, partition, n, task.NumPartition)
			for i := 0; i < n; i++ {
				p := partition[i]
				for j, vec := range partitionv[p] {
					vec.Index(lens[p]).Set(in[j].Index(i))
				}
				lens[p]++
				if lens[p] == defaultChunksize {
					if err := partitions[p].Encode(partitionv[p]...); err != nil {
						return err
					}
					lens[p] = 0
				}
			}
			if err == EOF {
				break
			}
		}
		// Flush remaining data.
		for p, n := range lens {
			if n == 0 {
				continue
			}
			limit(partitionv[p], n)
			if err := partitions[p].Encode(partitionv[p]...); err != nil {
				return err
			}
		}
	}

	for _, part := range partitions {
		if err := part.buf.Flush(); err != nil {
			return err
		}
		if err := part.closer.Close(); err != nil {
			return err
		}
	}
	// Disable defer close.
	partitions = nil
	return nil
}

// TaskReadRequest describes a request to read a slice buffer.
type taskReadRequest struct {
	// Task is the name of the task whose output is to be read.
	Task string
	// Partition is the partition number to read.
	Partition int
}

// Read reads a slice
func (w *worker) Read(ctx context.Context, req taskReadRequest, rc *io.ReadCloser) (err error) {
	*rc, err = w.store.Open(ctx, req.Task, req.Partition)
	return
}

func makeOutputVectors(task *Task) []reflect.Value {
	cols := make([]reflect.Value, len(task.Out))
	for i, typ := range task.Out {
		cols[i] = reflect.MakeSlice(reflect.SliceOf(typ), defaultChunksize, defaultChunksize)
	}
	return cols
}

func limit(cols []reflect.Value, n int) {
	if cols[0].Len() == n {
		return
	}
	for i := range cols {
		// TODO(marius): if we used addressable values here, we could
		// call Value.SetLen directly, avoiding this copy.
		cols[i] = cols[i].Slice(0, n)
	}
}

func unlimit(cols []reflect.Value) {
	c := cols[0].Cap()
	if c == cols[0].Len() {
		return
	}
	for i := range cols {
		cols[i] = cols[i].Slice(0, c)
	}
}
