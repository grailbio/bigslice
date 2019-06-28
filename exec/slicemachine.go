// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grailbio/base/backgroundcontext"
	"github.com/grailbio/base/data"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/status"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigslice/stats"
	"golang.org/x/sync/errgroup"
)

const (
	// ProbationTimeout is the amount of time that a machine will
	// remain in probation without being explicitly marked healthy.
	probationTimeout = 30 * time.Second

	// maxStartMachines is the maximum number of machines that
	// may be started in one batch.
	maxStartMachines = 10
)

// MachineHealth is the overall assessment of machine health by
// the bigmachine executor.
type machineHealth int

const (
	machineOk machineHealth = iota
	machineProbation
	machineLost
)

// SliceMachine manages a single bigmachine.Machine instance.
type sliceMachine struct {
	*bigmachine.Machine

	// Compiles ensures that each invocation is compiled exactly once on
	// the machine.
	Compiles once.Map

	// Commits keeps track of which combine keys have been committed
	// on the machine, so that they are run exactly once on the machine.
	Commits once.Map

	Stats  *stats.Map
	Status *status.Task

	// curprocs is the current number of procs on the machine that have
	// tasks assigned. curprocs is managed by the machineManager.
	curprocs int

	// health is managed by the machineManager.
	health machineHealth

	// lastFailure is managed by the machineManager.
	lastFailure time.Time

	// index is the machine's index in the executor's priority queue.
	index int

	donec chan machineDone

	mu sync.Mutex

	// Lost indicates whether the machine is considered lost as per
	// bigmachine.
	lost bool

	// Tasks is the set of tasks that have been run on this machine.
	// It is used to mark tasks lost when a machine fails.
	tasks []*Task

	disk bigmachine.DiskInfo
	mem  bigmachine.MemInfo
	load bigmachine.LoadInfo
	vals stats.Values
}

func (s *sliceMachine) String() string {
	var health string
	switch s.health {
	case machineOk:
		health = "ok"
	case machineProbation:
		health = "probation"
	case machineLost:
		health = "lost"
	}
	return fmt.Sprintf("%s (%s)", s.Addr, health)
}

// Done returns a proc on the machine, and reports any error
// observed while running tasks.
func (s *sliceMachine) Done(err error) {
	s.donec <- machineDone{s, err}
}

// Assign assigns the provided task to this machine. If the machine
// fails, its assigned tasks are marked LOST.
func (s *sliceMachine) Assign(task *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.health == machineLost {
		task.Set(TaskLost)
	} else {
		s.tasks = append(s.tasks)
	}
}

// Go manages a sliceMachine: it polls stats at regular intervals and
// marks tasks as lost when a machine fails.
func (s *sliceMachine) Go(ctx context.Context) error {
	stopped := s.Wait(bigmachine.Stopped)
loop:
	for ctx.Err() == nil {
		tctx, cancel := context.WithTimeout(ctx, statTimeout)
		g, gctx := errgroup.WithContext(tctx)
		var (
			mem  bigmachine.MemInfo
			merr error
			disk bigmachine.DiskInfo
			derr error
			load bigmachine.LoadInfo
			lerr error
			vals stats.Values
			verr error
		)
		g.Go(func() error {
			mem, merr = s.Machine.MemInfo(gctx, false)
			return nil
		})
		g.Go(func() error {
			disk, derr = s.Machine.DiskInfo(gctx)
			return nil
		})
		g.Go(func() error {
			load, lerr = s.Machine.LoadInfo(gctx)
			return nil
		})
		g.Go(func() error {
			verr = s.Machine.Call(ctx, "Worker.Stats", struct{}{}, &vals)
			return nil
		})
		_ = g.Wait()
		cancel()
		if merr != nil {
			log.Printf("meminfo %s: %v", s.Machine.Addr, merr)
		}
		if derr != nil {
			log.Printf("diskinfo %s: %v", s.Machine.Addr, derr)
		}
		if lerr != nil {
			log.Printf("loadinfo %s: %v", s.Machine.Addr, lerr)
		}
		if verr != nil {
			log.Printf("stats %s: %v", s.Machine.Addr, verr)
		}
		s.mu.Lock()
		if merr == nil {
			s.mem = mem
		}
		if derr == nil {
			s.disk = disk
		}
		if lerr == nil {
			s.load = load
		}
		if verr == nil {
			s.vals = vals
		}
		s.mu.Unlock()
		s.UpdateStatus()
		select {
		case <-time.After(statsPollInterval):
		case <-ctx.Done():
		case <-stopped:
			break loop
		}
	}
	// The machine is dead: mark it as such and also mark all of its pending
	// and completed tasks as lost.
	log.Error.Printf("lost machine %s: marking its tasks as LOST", s.Machine.Addr)
	s.mu.Lock()
	s.lost = true
	tasks := s.tasks
	s.tasks = nil
	s.mu.Unlock()
	for _, task := range tasks {
		task.Set(TaskLost)
	}
	return ctx.Err()
}

// Lost reports whether this machine is considered lost.
func (s *sliceMachine) Lost() bool {
	s.mu.Lock()
	lost := s.lost
	s.mu.Unlock()
	return lost
}

// UpdateStatus updates the machine's status.
func (s *sliceMachine) UpdateStatus() {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := s.vals.Copy()
	s.Stats.AddAll(values)
	var health string
	switch s.health {
	case machineOk:
	case machineProbation:
		health = " (probation)"
	case machineLost:
		health = " (lost)"
	}
	s.Status.Printf("mem %s/%s disk %s/%s load %.1f/%.1f/%.1f counters %s%s",
		data.Size(s.mem.System.Used), data.Size(s.mem.System.Total),
		data.Size(s.disk.Usage.Used), data.Size(s.disk.Usage.Total),
		s.load.Averages.Load1, s.load.Averages.Load5, s.load.Averages.Load15,
		values, health,
	)
}

// Load returns the machine's load, i.e., the proportion of its
// capacity that is currently in use.
func (s *sliceMachine) Load() float64 {
	return float64(s.curprocs) / float64(s.Maxprocs)
}

// MachineQ is a priority queue for sliceMachines, prioritized
// by the machine's load, as defined by (*sliceMachine).Load()
type machineQ []*sliceMachine

func (h machineQ) Len() int           { return len(h) }
func (h machineQ) Less(i, j int) bool { return h[i].Load() < h[j].Load() }
func (h machineQ) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *machineQ) Push(x interface{}) {
	m := x.(*sliceMachine)
	m.index = len(*h)
	*h = append(*h, m)
}

func (h *machineQ) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	x.index = -1
	return x
}

// MachineQ is a priority queue for sliceMachines, prioritized by the
// machine's last failure time, as defined by
// (*sliceMachine).LastFailure.
type machineFailureQ []*sliceMachine

func (h machineFailureQ) Len() int           { return len(h) }
func (h machineFailureQ) Less(i, j int) bool { return h[i].lastFailure.Before(h[j].lastFailure) }
func (h machineFailureQ) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *machineFailureQ) Push(x interface{}) {
	m := x.(*sliceMachine)
	m.index = len(*h)
	*h = append(*h, m)
}

func (h *machineFailureQ) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	x.index = -1
	return x
}

// MachineDone is used to report that a machine's request is done, along
// with an error used to gauge the machine's health.
type machineDone struct {
	*sliceMachine
	Err error
}

// startError is used to signal machine start errors.
type startError struct {
	Nproc int   // the number of procs that failed to start
	Err   error // the underlying error
}

// MachineManager manages a cluster of sliceMachines, load balancing requests
// among them. MachineManagers are constructed newMachineManager.
type machineManager struct {
	b       *bigmachine.B
	params  []bigmachine.Param
	group   *status.Group
	maxp    int
	maxLoad float64
	worker  *worker
	needc   chan int
	offerc  chan *sliceMachine
}

// NewMachineManager returns a new machineManager paramterized by the
// provided arguments. Maxp determines the maximum number of procs
// that may be allocated, maxLoad determines the maximum fraction of
// machine procs that may be allocated to user work.
//
// The cluster is not managed until machineManager.Do is called by the user.
func newMachineManager(b *bigmachine.B, params []bigmachine.Param, group *status.Group, maxp int, maxLoad float64, worker *worker) *machineManager {
	// Adjust maxLoad so that we are guaranteed at least one proc per
	// machine; otherwise we can get stuck in nasty deadlocks. We also
	// adjust maxp in this case to account for the fact, when maxLoad=0,
	// we should allocate the entirety of the machine towards a task
	// with internal parallelism, and the maxp should count towards
	// that.
	//
	// TODO(marius): maxp is still applied on a per-manager basis. It
	// should be shared across all managers, though this complicates
	// matters because, without de-allocating machines from one cluster
	// to another, or at least draining them and transferring them, we
	// could run into deadlocks. We should probably re-think cluster
	// management to better accommodate for this.
	maxprocs := b.System().Maxprocs()
	if machprocs := float64(maxprocs) * maxLoad; machprocs < 1 {
		maxLoad = 1 / float64(maxprocs)
		maxp = (maxp + maxprocs - 1) / maxprocs
	}
	return &machineManager{
		b:       b,
		params:  params,
		group:   group,
		maxp:    maxp,
		maxLoad: maxLoad,
		worker:  worker,
		needc:   make(chan int),
		offerc:  make(chan *sliceMachine),
	}
}

// Offer is a channel to which idle sliceMachines are sent.
// Each send indicates an idle capacity of one proc.
func (m *machineManager) Offer() <-chan *sliceMachine {
	return m.offerc
}

// Need indicates that an additional n procs are needed.
// Requests are canceled by passing negative values.
func (m *machineManager) Need(n int) {
	m.needc <- n
}

// Do starts machine management. The user typically calls this
// asynchronously. Do services requests for machine capacity and
// monitors machine health: stopped machines are considered lost and
// removed from management.
//
// Do attempts to maintain at least as many procs as are currently
// needed (as indicated by client's calls to Need); thus when a
// machine is lost, it may be replaced with another should it be
// needed.
func (m *machineManager) Do(ctx context.Context) {
	var (
		need, pending int
		// Scale each machine's maxprocs by the max load factor so that
		// maxp is interpreted as the maximum number of usable procs.
		machprocs      = max(1, int(float64(m.b.System().Maxprocs())*m.maxLoad))
		starterrc      = make(chan startError)
		startc         = make(chan []*sliceMachine)
		stoppedc       = make(chan *sliceMachine)
		donec          = make(chan machineDone)
		machines       machineQ
		probation      machineFailureQ
		probationTimer *time.Timer
	)
	// Scale maxp up by the load slack so that we don't over or underallocate.
	for {
		var (
			mach  *sliceMachine
			machc chan<- *sliceMachine
		)
		if len(machines) > 0 && machines[0].Load() < m.maxLoad {
			mach = machines[0]
			machc = m.offerc
		}
		if len(probation) == 0 {
			probationTimer = nil
		} else if probationTimer == nil {
			probationTimer = time.NewTimer(time.Until(probation[0].lastFailure.Add(probationTimeout)))
		}
		var timec <-chan time.Time
		if probationTimer != nil {
			timec = probationTimer.C
		}
		select {
		case machc <- mach:
			mach.curprocs++
			heap.Fix(&machines, mach.index)
		case <-timec:
			mach := probation[0]
			mach.health = machineOk
			log.Printf("removing machine %s from probation", mach.Addr)
			heap.Remove(&probation, 0)
			heap.Push(&machines, mach)
		case done := <-donec:
			mach := done.sliceMachine
			mach.curprocs--
			switch {
			case done.Err != nil && mach.health == machineOk:
				log.Error.Printf("putting machine %s on probation after error: %v", mach, done.Err)
				mach.health = machineProbation
				heap.Remove(&machines, mach.index)
				mach.lastFailure = time.Now()
				heap.Push(&probation, mach)
			case done.Err == nil && mach.health == machineProbation:
				log.Printf("machine %s returned successful result; removing probation", mach)
				mach.health = machineOk
				heap.Remove(&probation, mach.index)
				heap.Push(&machines, mach)
			case mach.health == machineLost:
				// In this case, the machine has already been removed from the heap.
			case mach.health == machineProbation:
				mach.lastFailure = time.Now()
				heap.Fix(&probation, mach.index)
			case mach.health == machineOk:
				heap.Fix(&machines, mach.index)
			default:
				panic("invalid machine state")
			}
		case n := <-m.needc:
			need += n
		case startErr := <-starterrc:
			log.Error.Printf("error starting machines: %v", startErr.Err)
			pending -= startErr.Nproc
		case started := <-startc:
			pending -= machprocs * len(started)
			for _, mach := range started {
				heap.Push(&machines, mach)
				mach.donec = donec
				go func(mach *sliceMachine) {
					<-mach.Wait(bigmachine.Stopped)
					stoppedc <- mach
				}(mach)
			}
		case mach := <-stoppedc:
			// Remove the machine from management. We let the sliceMachine
			// instance deal with failing the tasks.
			log.Error.Printf("machine %s stopped with error %s", mach, mach.Err())
			switch mach.health {
			case machineOk:
				heap.Remove(&machines, mach.index)
			case machineProbation:
				heap.Remove(&probation, mach.index)
			}
			mach.health = machineLost
			mach.Status.Done()
		case <-ctx.Done():
			return
		}

		// TODO(marius): consider scaling down when we don't need as many
		// resources any more; his would involve moving results to other
		// machines or to another storage medium.
		if have := (len(machines) + len(probation)) * machprocs; have+pending < need && have+pending < m.maxp {
			var (
				needProcs    = min(need, m.maxp) - have - pending
				needMachines = min((needProcs+machprocs-1)/machprocs, maxStartMachines)
			)
			pending += needMachines * machprocs
			log.Printf("slicemachine: %d machines (%d procs); %d machines pending (%d procs)",
				have/machprocs, have, pending/machprocs, pending)
			go func() {
				machines, err := startMachines(ctx, m.b, m.group, needMachines, m.worker, m.params...)
				if err != nil {
					starterrc <- startError{needMachines * machprocs, err}
				} else {
					startc <- machines
				}
			}()
		}
	}
}

// StartMachines starts a number of machines on b, installing a worker
// service on each of them. StartMachines returns when all of the machines
// are in bigmachine.Running state.
func startMachines(ctx context.Context, b *bigmachine.B, group *status.Group, n int, worker *worker, params ...bigmachine.Param) ([]*sliceMachine, error) {
	params = append([]bigmachine.Param{bigmachine.Services{"Worker": worker}}, params...)
	machines, err := b.Start(ctx, n, params...)
	if err != nil {
		return nil, err
	}
	g, ctx := errgroup.WithContext(ctx)
	slicemachines := make([]*sliceMachine, len(machines))
	for i := range machines {
		m, i := machines[i], i
		status := group.Start()
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
			go sm.Go(backgroundcontext.Get())
			slicemachines[i] = sm
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	n = 0
	for _, m := range slicemachines {
		if m != nil {
			slicemachines[n] = m
			n++
		}
	}
	return slicemachines[:n], nil
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
