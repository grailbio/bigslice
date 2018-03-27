// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"runtime/debug"
	"sync"

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
	go func() {
		log.Printf("http.ListenAndServe: %v", http.ListenAndServe(":3333", nil))
	}()

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
		panic("no such task")
	}
	return &workerReader{
		task:      task.Name,
		partition: partition,
		machine:   m,
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
			b.machinesErr = errors.New("no machines started")
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
	buffers     map[string]taskBuffer
	invocations map[uint64]bool
	tasks       map[string]*Task
}

func (w *worker) Init(b *bigmachine.B) error {
	w.tasks = make(map[string]*Task)
	w.invocations = make(map[uint64]bool)
	w.buffers = make(map[string]taskBuffer)
	w.b = b
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
			w.mu.Lock()
			// If we have it locally, use it directly.
			if buf := w.buffers[deptask.Name]; buf != nil {
				reader.q[j] = buf.Reader(dep.Partition)
			}
			w.mu.Unlock()
			if reader.q[j] != nil {
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
			reader.q[j] = &workerReader{
				task:      deptask.Name,
				partition: dep.Partition,
				machine:   machine,
			}
		}
		in[i] = reader
	}

	// Compute the task and store its output in a local buffer.
	out := task.Do(in)
	buf, err := bufferOutput(task, out)
	if err != nil {
		log.Printf("error: %v", err)
		return err
	}
	w.mu.Lock()
	w.buffers[task.Name] = buf
	w.mu.Unlock()
	return nil
}

// TaskReadRequest describes a request to read a slice of a task
// output buffer.
type taskReadRequest struct {
	// Task is the name of the task whose output is to be read.
	Task string
	// Partition is the partition number to read.
	Partition int
	// Off is the read offset.
	Off int
	// Max is the largest number of elements that should be returned.
	Max int
}

// TaskReadReply contains the reply for a single taskReadRequest.
type taskReadReply struct {
	// Data is the raw, encoded data.
	Data []byte
	// N is the number of elements returned. N < 0
	// indicates EOF.
	N int

	// TODO(marius): return a cookie to make paging (much) cheaper
}

// Read reads a slice
func (w *worker) Read(ctx context.Context, req taskReadRequest, reply *taskReadReply) (err error) {
	w.mu.Lock()
	buf := w.buffers[req.Task]
	w.mu.Unlock()
	if buf == nil {
		return fmt.Errorf("no buffer for task %s", req.Task)
	}
	columns, off := buf.Slice(req.Partition, req.Off)
	if off < 0 {
		reply.N = -1 // EOF
		return nil
	}
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	n := columns[0].Len() - off
	if req.Max < n {
		n = req.Max
	}
	// Encode a column at a time.
	for _, colv := range columns {
		if err := enc.EncodeValue(colv.Slice(off, off+n)); err != nil {
			return err
		}
	}
	reply.Data = b.Bytes()
	reply.N = n
	return nil
}

// A workerReader implements a Reader that reads data
// from a remote worker service.
type workerReader struct {
	task      string
	partition int
	machine   *bigmachine.Machine
	off       int
}

func (w *workerReader) Read(columns ...reflect.Value) (int, error) {
	ctx := context.Background()
	req := taskReadRequest{
		Task:      w.task,
		Partition: w.partition,
		Off:       w.off,
		Max:       columns[0].Len(),
	}
	var reply taskReadReply
	if err := w.machine.Call(ctx, "Worker.Read", req, &reply); err != nil {
		return 0, err
	}
	if reply.N < 0 {
		return 0, EOF
	}
	// TODO(marius): stream this straight through
	dec := gob.NewDecoder(bytes.NewReader(reply.Data))
	// Write each column sequentially.
	for _, column := range columns {
		ptr := reflect.New(column.Type())
		ptr.Elem().Set(column)
		if err := dec.DecodeValue(ptr); err != nil {
			return 0, err
		}
	}
	w.off += reply.N
	return reply.N, nil
}
