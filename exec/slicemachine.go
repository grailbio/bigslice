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

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigslice/stats"
	"golang.org/x/sync/errgroup"
)

const (
	// ProbationTimeout is the amount of time that a machine will
	// remain in probation without being explicitly marked healthy.
	probationTimeout = 30 * time.Second

	// MaxLoad is the maximum per-machine load.
	maxLoad = 0.95
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
	Compiles taskOnce

	// Commits keeps track of which combine keys have been committed
	// on the machine, so that they are run exactly once on the machine.
	Commits taskOnce

	Stats  *stats.Map
	Status *status.Task

	// curprocs is the current number of procs on the machine that have
	// tasks assigned. curprocs is managed by the manageMachines.
	curprocs int

	// health is managed by the manageMachines.
	health machineHealth

	// lastFailure is managed by the manageMachines.
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
	values := make(stats.Values)
	s.Stats.AddAll(values)
	var health string
	switch s.health {
	case machineOk:
	case machineProbation:
		health = " (probation)"
	case machineLost:
		health = " (lost)"
	}
	s.mu.Lock()
	s.Status.Printf("mem %s/%s disk %s/%s load %.1f/%.1f/%.1f counters %s%s",
		data.Size(s.mem.System.Used), data.Size(s.mem.System.Total),
		data.Size(s.disk.Usage.Used), data.Size(s.disk.Usage.Total),
		s.load.Averages.Load1, s.load.Averages.Load5, s.load.Averages.Load15,
		values, health,
	)
	s.mu.Unlock()
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

// ManageMachines manages a cluster of sliceMachines until the
// provided context is canceled. Maxp determines the maximum number
// of procs that may be allocated; needc is a channel that indicates
// an (additive) number of procs needed. The currently least loaded,
// healthy machine is offered on channel offerc.
//
// After a machine is retrieved by way of offerc, the client
// dispatches an operation on it. Clients call (*sliceMachine).Done
// to indicate that the operation was completed, freeing a proc on
// the machine. Clients may also report a task-related error, which
// is used to put the machine on probation. Once a machine is on
// probation, it is taken out after either (1) a successful result is
// reported, or (2) a timeout expires.
//
// ManageMachines also monitors machine health through the
// bigmachine-reported status; machines that are stopped are
// considered lost and taken out of rotation.
//
// ManageMachines attempts to maintain at least as many procs
// as are currently needed (as indicated by client's requests to
// channel needc); thus when a machine is lost, it may be replaced
// with an other should it be needed.
func manageMachines(ctx context.Context, b *bigmachine.B, group *status.Group, maxp int, needc <-chan int, offerc chan<- *sliceMachine) {
	var (
		need, pending  int
		maxprocs       = b.System().Maxprocs()
		starterrc      = make(chan error)
		startc         = make(chan []*sliceMachine)
		stoppedc       = make(chan *sliceMachine)
		donec          = make(chan machineDone)
		machines       machineQ
		probation      machineFailureQ
		probationTimer *time.Timer
	)
	for {
		var m *sliceMachine
		var mc chan<- *sliceMachine
		if len(machines) > 0 && machines[0].Load() < 0.95 {
			m = machines[0]
			mc = offerc
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
		case mc <- m:
			m.curprocs++
			heap.Fix(&machines, m.index)
		case <-timec:
			m := probation[0]
			m.health = machineOk
			log.Printf("removing machine %s from probation", m.Addr)
			heap.Remove(&probation, 0)
			heap.Push(&machines, m)
		case done := <-donec:
			m := done.sliceMachine
			m.curprocs--
			switch {
			case done.Err != nil && m.health == machineOk:
				log.Error.Printf("putting machine %s on probation after error: %v", m, done.Err)
				m.health = machineProbation
				heap.Remove(&machines, m.index)
				m.lastFailure = time.Now()
				heap.Push(&probation, m)
			case done.Err == nil && m.health == machineProbation:
				log.Printf("machine %s returned successful result; removing probation", m)
				m.health = machineOk
				heap.Remove(&probation, m.index)
				heap.Push(&machines, m)
			case m.health == machineLost:
				// In this case, the machine has already been removed from the heap.
			case m.health == machineProbation:
				m.lastFailure = time.Now()
				heap.Fix(&probation, m.index)
			case m.health == machineOk:
				heap.Fix(&machines, m.index)
			default:
				panic("invalid machine state")
			}
		case n := <-needc:
			need += n
		case err := <-starterrc:
			log.Error.Printf("error starting machines: %v", err)
			pending -= maxprocs
		case started := <-startc:
			pending -= maxprocs
			for _, m := range started {
				heap.Push(&machines, m)
				m.donec = donec
				go func(m *sliceMachine) {
					<-m.Wait(bigmachine.Stopped)
					stoppedc <- m
				}(m)
			}
		case m := <-stoppedc:
			// Remove the machine from management. We let the sliceMachine
			// instance deal with failing the tasks.
			log.Error.Printf("machine %s stopped with error %s", m, m.Err())
			switch m.health {
			case machineOk:
				heap.Remove(&machines, m.index)
			case machineProbation:
				heap.Remove(&probation, m.index)
			}
			m.health = machineLost
			m.Status.Done()
		case <-ctx.Done():
			return
		}

		// TODO(marius): consider scaling down when we don't need as many
		// resources any more; his would involve moving results to other
		// machines or to another storage medium.
		if have := (len(machines) + len(probation)) * maxprocs; have+pending < need && have+pending < maxp {
			pending += maxprocs
			log.Printf("slicemachine: %d machines (%d procs); %d machines pending (%d procs)",
				have/maxprocs, have, pending/maxprocs, pending)
			go func() {
				machines, err := startMachines(ctx, b, group, 1)
				if err != nil {
					starterrc <- err
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
func startMachines(ctx context.Context, b *bigmachine.B, group *status.Group, n int) ([]*sliceMachine, error) {
	machines, err := b.Start(ctx, n, bigmachine.Services{"Worker": &worker{}})
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
			go sm.Go(context.Background())
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
