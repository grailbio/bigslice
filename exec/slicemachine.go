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
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/status"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/stats"
	"golang.org/x/sync/errgroup"
)

// ProbationTimeout is the amount of time that a machine will
// remain in probation without being explicitly marked healthy.
var ProbationTimeout = 30 * time.Second

// maxStartMachines is the maximum number of machines that
// may be started in one batch.
const maxStartMachines = 10

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

	// maxTaskProcs is the maximum number of procs on the machine to which tasks
	// can be assigned. This can be different from Maxprocs, as it is attenuated
	// by (*machineManager).Maxload.
	maxTaskProcs int

	// taskProcs is the current number of procs on the machine that have tasks
	// assigned. taskProcs is managed by the machineManager.
	taskProcs int

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

// Done returns procs on the machine, and reports any error observed while
// running tasks.
func (s *sliceMachine) Done(procs int, err error) {
	s.donec <- machineDone{s, procs, err}
}

// Assign assigns the provided task to this machine. If the machine
// fails, its assigned tasks are marked LOST.
func (s *sliceMachine) Assign(task *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lost {
		task.Set(TaskLost)
	} else {
		s.tasks = append(s.tasks, task)
	}
}

// Go manages a sliceMachine: it polls stats at regular intervals and
// marks tasks as lost when a machine fails.
func (s *sliceMachine) Go(ctx context.Context) {
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
	s.mu.Lock()
	s.lost = true
	tasks := s.tasks
	s.tasks = nil
	s.mu.Unlock()
	log.Error.Printf("lost machine %s: marking its %d tasks as LOST", s.Machine.Addr, len(tasks))
	for _, task := range tasks {
		task.Set(TaskLost)
	}
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
	return float64(s.taskProcs) / float64(s.maxTaskProcs)
}

// machineFailureQ is a priority queue for sliceMachines, prioritized by the
// machine's last failure time, as defined by (*sliceMachine).LastFailure.
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

// timer is a wrapper around time.Timer with an API convenient for managing
// probation timeouts.
type timer struct {
	// t is the underlying *time.Timer. It may be nil.
	t *time.Timer
	// at is the time (or later) at which t expired or will expire, if t is
	// non-nil.
	at time.Time
}

// Clear clears t; subsequent calls to C() will return nil. If t is already
// cleared, no-op.
func (t *timer) Clear() {
	if t.t == nil {
		return
	}
	t.t.Stop()
	t.t = nil
}

// Set sets t to expire at at. If the timer was already set to expire at at,
// no-op, even if the timer has already expired.
func (t *timer) Set(at time.Time) {
	if t.t == nil {
		t.at = at
		t.t = time.NewTimer(time.Until(at))
		return
	}
	if t.at == at {
		return
	}
	if !t.t.Stop() {
		<-t.t.C
	}
	t.at = at
	t.t.Reset(time.Until(at))
}

// C returns a channel on which the current time is sent when t expires. If t is
// cleared, returns nil.
func (t *timer) C() <-chan time.Time {
	if t.t == nil {
		return nil
	}
	return t.t.C
}

// MachineDone is used to report that a machine's request is done, along
// with an error used to gauge the machine's health.
type machineDone struct {
	*sliceMachine
	// procs is the number of procs to be returned to the pool available for
	// task assignment on the machine.
	procs int
	Err   error
}

// startResult is used to signal the result of attempts to start machines.
type startResult struct {
	// machines is a slice of the machines that were successfully started.
	machines []*sliceMachine
	// nFailures is the number of machines that we attempted but failed to
	// start.
	nFailures int
}

// MachineManager manages a cluster of sliceMachines, load balancing requests
// among them. MachineManagers are constructed newMachineManager.
type machineManager struct {
	b      *bigmachine.B
	params []bigmachine.Param
	group  *status.Group
	maxp   int
	// machprocs is the number of procs each managed machine has available for
	// tasks, taking into account max load.
	machprocs int
	worker    *worker
	// schedQ is the priority queue of scheduling requests, which determines the
	// order in which requests are satisfied. See Offer.
	schedQ   scheduleRequestQ
	schedc   chan scheduleRequest
	unschedc chan scheduleRequest

	machinesWG sync.WaitGroup
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
	machprocs := int(float64(maxprocs) * maxLoad)
	if machprocs < 1 {
		machprocs = 1
		maxp = (maxp + maxprocs - 1) / maxprocs
	}
	return &machineManager{
		b:         b,
		params:    params,
		group:     group,
		maxp:      maxp,
		machprocs: machprocs,
		worker:    worker,
		schedc:    make(chan scheduleRequest),
		unschedc:  make(chan scheduleRequest),
	}
}

// Offer asks m to offer a machine on which to run work with the given priority
// and number of procs. When m schedules the request, the machine is sent to the
// returned channel. The second return value is a function that cancels the
// request when called. If the request has already been serviced (i.e. a machine
// has already been delivered), calling the cancel function is a no-op.
func (m *machineManager) Offer(priority, procs int) (<-chan *sliceMachine, func()) {
	machc := make(chan *sliceMachine)
	s := scheduleRequest{
		procs:    procs,
		priority: priority,
		machc:    machc,
	}
	m.schedc <- s
	cancel := func() {
		m.unschedc <- s
	}
	return machc, cancel
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
		need, pending  int
		startc         = make(chan startResult)
		stoppedc       = make(chan *sliceMachine)
		donec          = make(chan machineDone)
		machines       []*sliceMachine
		probation      machineFailureQ
		probationTimer timer
		// We track consecutive failures to start machines as a heuristic to
		// decide that there might be a systematic problem preventing machines
		// from starting.
		consecutiveStartFailures int
	)
	for {
		var (
			mach  *sliceMachine
			machc chan<- *sliceMachine
		)
		if len(m.schedQ) > 0 {
			mach, machc = schedule(m.schedQ[0], machines)
		}
		if len(probation) == 0 {
			probationTimer.Clear()
		} else {
			probationTimer.Set(probation[0].lastFailure.Add(ProbationTimeout))
		}
		select {
		case machc <- mach:
			mach.taskProcs += m.schedQ[0].procs
			heap.Pop(&m.schedQ)
		case <-probationTimer.C():
			mach := probation[0]
			mach.health = machineOk
			log.Printf("removing machine %s from probation", mach.Addr)
			heap.Remove(&probation, 0)
			machines = appendMachine(machines, mach)
			probationTimer.Clear()
		case done := <-donec:
			need -= done.procs
			mach := done.sliceMachine
			mach.taskProcs -= done.procs
			switch {
			case done.Err != nil && !errors.Is(errors.Remote, done.Err) && mach.health == machineOk:
				// We only consider probation if we have problems with RPC
				// machinery, e.g. host unavailable or other network errors. If
				// the error is from application code of an RPC, we defer to the
				// evaluation engine for remediation. This is to limit the blast
				// radius of a problematic machine, e.g. a call to machine A
				// transitively calls machine B, but machine B is down; the call
				// to machine A will return an error, but we do not want to put
				// machine A on probation.
				log.Error.Printf("putting machine %s on probation after error: %v", mach, done.Err)
				mach.health = machineProbation
				machines = removeMachine(machines, mach)
				mach.lastFailure = time.Now()
				heap.Push(&probation, mach)
			case done.Err == nil && mach.health == machineProbation:
				log.Printf("machine %s returned successful result; removing probation", mach)
				mach.health = machineOk
				heap.Remove(&probation, mach.index)
				machines = appendMachine(machines, mach)
			case mach.health == machineLost:
				// In this case, the machine has already been removed from the heap.
			case mach.health == machineProbation:
				log.Error.Printf("keeping machine %s on probation after error: %v", mach, done.Err)
				mach.lastFailure = time.Now()
				heap.Fix(&probation, mach.index)
			case mach.health == machineOk:
				// Everything continues merrily.
			default:
				panic("invalid machine state")
			}
		case s := <-m.schedc:
			heap.Push(&m.schedQ, s)
			need += s.procs
		case s := <-m.unschedc:
			if s.index < 0 {
				// The scheduling request is no longer queued, which means
				// scheduling request has already been serviced.
				break
			}
			need -= s.procs
			heap.Remove(&m.schedQ, s.index)
		case result := <-startc:
			pending -= m.machprocs * (len(result.machines) + result.nFailures)
			for _, mach := range result.machines {
				machines = appendMachine(machines, mach)
				mach.donec = donec
				go func(mach *sliceMachine) {
					<-mach.Wait(bigmachine.Stopped)
					stoppedc <- mach
				}(mach)
			}
			if len(result.machines) > 0 {
				consecutiveStartFailures = 0
			} else {
				consecutiveStartFailures += result.nFailures
				if consecutiveStartFailures > 8 {
					log.Printf("warning; failed to start last %d machines; check for systematic problem preventing machine bootup", consecutiveStartFailures)
				}
			}
		case mach := <-stoppedc:
			// Remove the machine from management. We let the sliceMachine
			// instance deal with failing the tasks.
			log.Error.Printf("machine %s stopped with error %s", mach, mach.Err())
			switch mach.health {
			case machineOk:
				machines = removeMachine(machines, mach)
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
		if have := (len(machines) + len(probation)) * m.machprocs; have+pending < need && have+pending < m.maxp {
			var (
				needProcs    = min(need, m.maxp) - have - pending
				needMachines = min((needProcs+m.machprocs-1)/m.machprocs, maxStartMachines)
			)
			pending += needMachines * m.machprocs
			log.Printf("slicemachine: %d machines (%d procs); %d machines pending (%d procs)",
				have/m.machprocs, have, pending/m.machprocs, pending)
			go func() {
				machines := startMachines(ctx, m.b, m.group, m.machprocs, needMachines, m.worker, m.params...)
				for _, machine := range machines {
					machine := machine
					m.machinesWG.Add(1)
					go func() {
						machine.Go(ctx)
						m.machinesWG.Done()
					}()
				}
				startc <- startResult{
					machines:  machines,
					nFailures: needMachines - len(machines),
				}
			}()
		}
	}
}

// schedule attempts to schedule s on a machine in machines, returning the
// machine and the channel on which to send the machine. If no machine can
// satisfy the request, it returns (nil, nil).
func schedule(s scheduleRequest, machines []*sliceMachine) (*sliceMachine, chan<- *sliceMachine) {
	// schedQ is ordered from largest to smallest proc needs, within a given
	// priority, so this implements a first fit decreasing scheduling strategy.
	for _, m := range machines {
		freeProcs := m.maxTaskProcs - m.taskProcs
		if s.procs <= freeProcs {
			return m, s.machc
		}
	}
	return nil, nil
}

func appendMachine(ms []*sliceMachine, m *sliceMachine) []*sliceMachine {
	m.index = len(ms)
	return append(ms, m)
}

func removeMachine(ms []*sliceMachine, m *sliceMachine) []*sliceMachine {
	ms[m.index] = ms[len(ms)-1]
	ms[m.index].index = m.index
	m.index = -1
	return ms[:len(ms)-1]
}

// StartMachines starts a number of machines on b, installing a worker service
// on each of them. StartMachines returns a slice of successfully started
// machines when all of them are in bigmachine.Running state. If a machine
// fails to start, it is not included.
func startMachines(ctx context.Context, b *bigmachine.B, group *status.Group, maxTaskProcs int, n int, worker *worker, params ...bigmachine.Param) []*sliceMachine {
	params = append([]bigmachine.Param{bigmachine.Services{"Worker": worker}}, params...)
	machines, err := b.Start(ctx, n, params...)
	if err != nil {
		log.Error.Printf("error starting machines: %v", err)
		return nil
	}
	var wg sync.WaitGroup
	slicemachines := make([]*sliceMachine, len(machines))
	for i := range machines {
		i := i
		m := machines[i]
		status := group.Start()
		status.Print("waiting for machine to boot")
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-m.Wait(bigmachine.Running)
			if err := m.Err(); err != nil {
				log.Printf("machine %s failed to start: %v", m.Addr, err)
				status.Printf("failed to start: %v", err)
				status.Done()
				return
			}
			var workerFuncLocs []string
			if err := m.RetryCall(ctx, "Worker.FuncLocations", struct{}{}, &workerFuncLocs); err != nil {
				status.Printf("failed to verify funcs")
				status.Done()
				m.Cancel()
				return
			}
			diff := bigslice.FuncLocationsDiff(bigslice.FuncLocations(), workerFuncLocs)
			if len(diff) > 0 {
				for _, edit := range diff {
					log.Printf("[funcsdiff] %s", edit)
				}
				log.Panicf("machine %s has different funcs; check for local or non-deterministic Func creation", m.Addr)
			}
			status.Title(m.Addr)
			status.Print("running")
			log.Printf("machine %v is ready", m.Addr)
			sm := &sliceMachine{
				Machine:      m,
				Stats:        stats.NewMap(),
				Status:       status,
				maxTaskProcs: maxTaskProcs,
			}
			slicemachines[i] = sm
		}()
	}
	wg.Wait()
	n = 0
	for _, m := range slicemachines {
		if m != nil {
			slicemachines[n] = m
			n++
		}
	}
	return slicemachines[:n]
}

type scheduleRequest struct {
	// priority is the priority of the request. Lower values have higher
	// priority. If there is more than one request waiting for a machine, the
	// request with the lowest priority value will be satisfied first.
	priority int
	// procs is the number of procs being requested.
	procs int
	machc chan *sliceMachine
	// index is the index of this request in the request heap.
	index int
}

// scheduleRequestQ is a priority queue based on request priority and proc
// demand.
type scheduleRequestQ []scheduleRequest

func (q scheduleRequestQ) Len() int { return len(q) }

func (q scheduleRequestQ) Less(i, j int) bool {
	if q[i].priority != q[j].priority {
		return q[i].priority < q[j].priority
	}
	// Higher proc demand comes first, as we implement first fit decreasing
	// scheduling.
	return q[i].procs > q[j].procs
}

func (q scheduleRequestQ) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *scheduleRequestQ) Push(x interface{}) {
	n := len(*q)
	s := x.(scheduleRequest)
	s.index = n
	*q = append(*q, s)
}

func (q *scheduleRequestQ) Pop() interface{} {
	old := *q
	n := len(old)
	s := old[n-1]
	s.index = -1
	*q = old[0 : n-1]
	return s
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
