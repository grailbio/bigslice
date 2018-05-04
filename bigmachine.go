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
	"runtime/debug"
	"sync"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigslice/stats"
	"golang.org/x/sync/errgroup"
)

const statsPollInterval = 5 * time.Second

func init() {
	gob.Register(&worker{})
}

// TODO(marius): clean up flag registration, etc. vis-a-vis bigmachine.
// e.g., perhaps we can register flags in a bigmachine flagset that gets
// parsed together, so that we don't litter the process with global flags.

// SliceMachine maintains bigslice-specific metadata to bigmachine machines.
type sliceMachine struct {
	*bigmachine.Machine
	Load   int
	Stats  *stats.Map
	Status *status.Task

	mu   sync.Mutex
	disk bigmachine.DiskInfo
	mem  bigmachine.MemInfo
}

// Go polls runtime statistics from the underlying machine until
// the provided context is done.
func (s *sliceMachine) Go(ctx context.Context) error {
	for ctx.Err() == nil {
		g, gctx := errgroup.WithContext(ctx)
		var (
			mem  bigmachine.MemInfo
			merr error
			disk bigmachine.DiskInfo
			derr error
		)
		g.Go(func() error {
			mem, merr = s.Machine.MemInfo(gctx)
			return nil
		})
		g.Go(func() error {
			disk, derr = s.Machine.DiskInfo(gctx)
			return nil
		})
		if err := g.Wait(); err != nil {
			return err
		}
		if merr != nil {
			log.Printf("meminfo %s: %v", s.Machine.Addr, merr)
		}
		if derr != nil {
			log.Printf("diskinfo %s: %v", s.Machine.Addr, derr)
		}
		s.mu.Lock()
		if merr == nil {
			s.mem = mem
		}
		if derr == nil {
			s.disk = disk
		}
		s.mu.Unlock()
		s.UpdateStatus()
		select {
		case <-time.After(statsPollInterval):
		case <-ctx.Done():
		}
	}
	return ctx.Err()
}

// UpdateStatus update's the machine's status.
func (s *sliceMachine) UpdateStatus() {
	values := make(stats.Values)
	s.Stats.AddAll(values)
	s.mu.Lock()
	s.Status.Printf("mem %s disk %s counters %s", s.mem, s.disk, values)
	s.mu.Unlock()
}

type machineHeap []*sliceMachine

func (h machineHeap) Len() int           { return len(h) }
func (h machineHeap) Less(i, j int) bool { return h[i].Load < h[j].Load }
func (h machineHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *machineHeap) Push(x interface{}) {
	*h = append(*h, x.(*sliceMachine))
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

	machinesOnce sync.Once
	machines     machineHeap
	machinesErr  error

	status *status.Group

	mu        sync.Mutex
	locations map[*Task]*bigmachine.Machine
	stats     map[string]stats.Values
}

func newBigmachineExecutor(system bigmachine.System) *bigmachineExecutor {
	return &bigmachineExecutor{
		system: system,
	}
}

// Start starts registers the bigslice worker with bigmachine and then
// starts the bigmachine.
//
// TODO(marius): provide fine-grained fault tolerance.
func (b *bigmachineExecutor) Start(sess *Session) (shutdown func()) {
	b.sess = sess
	b.b = bigmachine.Start(b.system)
	b.locations = make(map[*Task]*bigmachine.Machine)
	b.stats = make(map[string]stats.Values)
	if status := sess.Status(); status != nil {
		b.status = status.Group("bigmachine")
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
	task.Status.Print("waiting for a machine")
	if err := b.initMachines(); err != nil {
		return err
	}
	b.mu.Lock()
	m := heap.Pop(&b.machines).(*sliceMachine)
	m.Load++
	heap.Push(&b.machines, m)
	b.mu.Unlock()

	numTasks := m.Stats.Int("tasks")
	numTasks.Add(1)
	m.UpdateStatus()
	defer func() {
		numTasks.Add(-1)
		m.UpdateStatus()
	}()

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
					log.Printf("Worker.Stats: %v", err)
				}
				return
			}
			task.Status.Printf("%s: %s", m.Addr, vals)
			b.mu.Lock()
			b.stats[task.Name] = vals
			b.mu.Unlock()
			b.updateStatus()
		}
	}()

	var reply taskRunReply
	err = m.Call(ctx, "Worker.Run", req, &reply)
	b.mu.Lock()
	m.Load--
	for i := range b.machines {
		if b.machines[i] == m {
			heap.Fix(&b.machines, i)
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
	tp := taskPartition{task.Name, partition}
	var rc io.ReadCloser
	if err := m.Call(ctx, "Worker.Read", tp, &rc); err != nil {
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
	b.mu.Lock()
	for _, mach := range b.machines {
		n += mach.Maxprocs
	}
	b.mu.Unlock()
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
			status := b.status.Start()
			status.Print("waiting for machine to boot")
			g.Go(func() error {
				<-m.Wait(bigmachine.Running)
				if err := m.Err(); err != nil {
					log.Printf("machine %s failed to start: %v", m.Addr, err)
					status.Printf("failed to start: %v", err)
					status.Done()
					return nil
				}
				status.Title(m.Addr)
				status.Print("running")
				log.Printf("machine %v is ready", m.Addr)
				sm := &sliceMachine{
					Machine: m,
					Stats:   stats.NewMap(),
					Status:  status,
				}
				// TODO(marius): pass a context that's tied to the evaluation
				// lifetime, or lifetime of the machine.
				go sm.Go(context.Background())
				b.mu.Lock()
				b.machines = append(b.machines, sm)
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
		heap.Init(&b.machines)
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
	stats       *stats.Map
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
	w.stats = stats.NewMap()
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
	recordsOut := w.stats.Int("write")
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
	var (
		totalRecordsIn *stats.Int
		recordsIn      *stats.Int
	)
	if len(task.Deps) > 0 {
		totalRecordsIn = w.stats.Int("inrecords")
		recordsIn = w.stats.Int("read")
	}
	in := make([]Reader, len(task.Deps))
	for i, dep := range task.Deps {
		reader := new(multiReader)
		reader.q = make([]Reader, len(dep.Tasks))
	Tasks:
		for j, deptask := range dep.Tasks {
			// If we have it locally, or if we're using a shared backend store
			// (e.g., S3), then read it directly.
			info, err := w.store.Stat(ctx, deptask.Name, dep.Partition)
			if err == nil {
				rc, err := w.store.Open(ctx, deptask.Name, dep.Partition)
				if err == nil {
					defer rc.Close()
					reader.q[j] = newDecodingReader(rc)
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
			var rc io.ReadCloser
			if err := machine.Call(ctx, "Worker.Read", tp, &rc); err != nil {
				return err
			}
			reader.q[j] = &statsReader{newDecodingReader(rc), recordsIn}
			totalRecordsIn.Add(info.Records)
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
		wc  WriteCommitter
		buf *bufio.Writer
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
		part.wc = wc
		part.buf = bufio.NewWriter(wc)
		part.Encoder = NewEncoder(part.buf)
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
		_, err := out.Read(ctx, nil)
		if err == EOF {
			err = nil
		}
		return err
	case task.Hasher != nil:
		// If we have a Hasher, we're expected to partition the output.
		var (
			partition   = make([]int, defaultChunksize)
			partitionv  = make([]Frame, task.NumPartition)
			lens        = make([]int, task.NumPartition)
			partitioner = newPartitioner(task.Hasher, task.NumPartition)
		)
		for i := range partitionv {
			partitionv[i] = MakeFrame(task, defaultChunksize)
		}
		in := MakeFrame(task, defaultChunksize)
		for {
			n, err := out.Read(ctx, in)
			if err != nil && err != EOF {
				return err
			}
			partitioner.Partition(in, partition)
			for i := 0; i < n; i++ {
				p := partition[i]
				for j, vec := range partitionv[p] {
					vec.Index(lens[p]).Set(in[j].Index(i))
				}
				lens[p]++
				count[p]++
				// Flush when we fill up.
				if lens[p] == defaultChunksize {
					if err := partitions[p].Encode(partitionv[p]); err != nil {
						return err
					}
					lens[p] = 0
				}
			}
			recordsOut.Add(int64(n))
			if err == EOF {
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
		if task.NumPartition != 1 {
			return fmt.Errorf("invalid task graph: NumPartition is %d, but no Hasher provided", task.NumPartition)
		}
		in := MakeFrame(task, defaultChunksize)
		for {
			n, err := out.Read(ctx, in)
			if err != nil && err != EOF {
				return err
			}
			if err := partitions[0].Encode(in.Slice(0, n)); err != nil {
				return err
			}
			recordsOut.Add(int64(n))
			count[0] += int64(n)
			if err == EOF {
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
func (w *worker) Stat(ctx context.Context, tp taskPartition, info *SliceInfo) (err error) {
	*info, err = w.store.Stat(ctx, tp.Task, tp.Partition)
	return
}

// Read reads a slice
func (w *worker) Read(ctx context.Context, tp taskPartition, rc *io.ReadCloser) (err error) {
	*rc, err = w.store.Open(ctx, tp.Task, tp.Partition)
	return
}