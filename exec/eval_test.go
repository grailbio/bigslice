// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	goerrors "errors"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/eventlog"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
	"golang.org/x/sync/errgroup"
)

type testExecutor struct{}

func (testExecutor) Name() string {
	return "test"
}

func (testExecutor) Start(*Session) (shutdown func()) {
	return func() {}
}

func (t testExecutor) Run(task *Task) {
	task.Lock()
	task.state = TaskRunning
	task.Broadcast()
	task.Unlock()
}

func (testExecutor) Reader(*Task, int) sliceio.ReadCloser {
	panic("not implemented")
}

func (testExecutor) Discard(context.Context, *Task) {}

func (testExecutor) Eventer() eventlog.Eventer {
	return eventlog.Nop{}
}

func (testExecutor) HandleDebug(handler *http.ServeMux) {
	panic("not implemented")
}

// constEvalTest sets up a 2-root-node task graph.
type constEvalTest struct {
	Tasks []*Task

	wg      sync.WaitGroup
	evalErr error
}

func (c *constEvalTest) Go(t *testing.T) {
	t.Helper()
	c.Tasks, _, _ = compileFunc(func() bigslice.Slice {
		return bigslice.Const(2, []int{1, 2, 3})
	})
	ctx := context.Background()
	c.wg.Add(1)
	go func() {
		c.evalErr = Eval(ctx, testExecutor{}, c.Tasks, nil)
		c.wg.Done()
	}()
}

func (c *constEvalTest) EvalErr() error {
	c.wg.Wait()
	return c.evalErr
}

// SimpleEvalTest sets up a simple, 2-node task graph.
type simpleEvalTest struct {
	Tasks []*Task

	ConstTask, CogroupTask *Task

	wg      sync.WaitGroup
	evalErr error
}

func (s *simpleEvalTest) Go(t *testing.T) {
	t.Helper()
	s.Tasks, _, _ = compileFunc(func() bigslice.Slice {
		slice := bigslice.Const(1, []int{1, 2, 3})
		slice = bigslice.Cogroup(slice)
		return slice
	})
	s.ConstTask = s.Tasks[0].Deps[0].Task(0)
	s.CogroupTask = s.Tasks[0]
	ctx := context.Background()
	s.wg.Add(1)
	go func() {
		s.evalErr = Eval(ctx, testExecutor{}, s.Tasks, nil)
		s.wg.Done()
	}()
}

func (s *simpleEvalTest) EvalErr() error {
	s.wg.Wait()
	return s.evalErr
}

func waitState(t *testing.T, task *Task, state TaskState) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task.Lock()
	defer task.Unlock()
	for task.state != state {
		if err := task.Wait(ctx); err != nil {
			t.Fatalf("task %v (state %v) did not reach desired state %v", task.Name, task.state, state)
		}
	}
}

// TestTaskErr verifies that a task evaluation error (TaskErr) causes Eval to
// return a corresponding error.
func TestTaskErr(t *testing.T) {
	var (
		test simpleEvalTest
		ctx  = context.Background()
	)
	test.Go(t)
	state, err := test.ConstTask.WaitState(ctx, TaskRunning)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := state, TaskRunning; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := test.CogroupTask.State(), TaskInit; got != want {
		t.Fatalf("got %v, want %v: %v", got, want, test.CogroupTask)
	}
	test.ConstTask.Error(goerrors.New("const task error"))

	err = test.EvalErr()
	if err == nil {
		t.Fatal("expected error")
	}
	if got, want := strings.Contains(err.Error(), "const task error"), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := test.CogroupTask.State(), TaskInit; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestAllRootsEvaluated verifies that all roots are evaluated at the moment
// Eval returns.
func TestAllRootsEvaluated(t *testing.T) {
	var (
		test constEvalTest
		ctx  = context.Background()
	)
	test.Go(t)
	// We have two root tasks, task0 and task1. task0 is evaluated
	// successfully. While task1 runs, task0 is lost. Verify that Eval only
	// returns once task0 is re-evaluated successfully.
	var (
		task0 = test.Tasks[0]
		task1 = test.Tasks[1]
	)
	// task0 is evaluated successfully.
	task0.Lock()
	for task0.state != TaskRunning {
		if err := task0.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	task0.state = TaskOk
	task0.Broadcast()
	task0.Unlock()
	// While task1 runs, task0 is lost.
	task1.Lock()
	for task1.state != TaskRunning {
		if err := task1.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	task1.Unlock()
	// Allow time for evaluation to notice task0's TaskOk state before marking
	// it lost.
	// TODO: Though this seems to work reliably in my environment, consider a
	// non-racy way of doing this. Note that this shouldn't ever cause the test
	// to falsely fail. It just means that this will test the Running -> Lost
	// path instead of the Running -> Ok -> Lost path, as the evaluator might
	// not see the transient Ok state.
	time.Sleep(1 * time.Millisecond)
	task0.Lock()
	task0.state = TaskLost
	task0.Broadcast()
	task0.Unlock()
	// task1 is successfully evaluated.
	task1.Lock()
	task1.state = TaskOk
	task1.Broadcast()
	task1.Unlock()
	task0.Lock()
	// Expect task0 to be resubmitted. Eval should not return until all roots
	// are successfully evaluated.
	for task0.state != TaskRunning {
		if err := task0.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	task0.state = TaskOk
	task0.Broadcast()
	task0.Unlock()
	if err := test.EvalErr(); err != nil {
		t.Fatal(err)
	}
}

func TestResubmitLostTask(t *testing.T) {
	var (
		test simpleEvalTest
		ctx  = context.Background()
	)
	test.Go(t)
	var (
		fst = test.ConstTask
		snd = test.CogroupTask
	)
	fst.Lock()
	for fst.state != TaskRunning {
		if err := fst.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	fst.state = TaskLost
	fst.Broadcast()
	for fst.state == TaskLost {
		if err := fst.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// The evaluator should have resubmitted it.
	if got, want := fst.state, TaskRunning; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Now we lose both of them while the second is running.
	// The evaluator should resubmit both.
	fst.state = TaskOk
	fst.Broadcast()
	fst.Unlock()

	snd.Lock()
	for snd.state != TaskRunning {
		if err := snd.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	fst.Lock()
	snd.state = TaskLost
	snd.Broadcast()
	snd.Unlock()
	fst.state = TaskLost
	fst.Broadcast()

	for fst.state < TaskRunning {
		if err := fst.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := snd.State(), TaskLost; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	fst.state = TaskOk
	fst.Broadcast()
	fst.Unlock()

	snd.Lock()
	for snd.state < TaskRunning {
		if err := snd.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	snd.state = TaskOk
	snd.Broadcast()
	snd.Unlock()

	if err := test.EvalErr(); err != nil {
		t.Fatal(err)
	}
}

func TestResubmitLostInteriorTask(t *testing.T) {
	for _, parallel := range []int{1, 10} {
		parallel := parallel
		t.Run(fmt.Sprintf("parallel=%v", parallel), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tasks, _, _ := compileFunc(func() (slice bigslice.Slice) {
				slice = bigslice.Const(2, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
				slice = bigslice.Cogroup(slice)
				return
			})

			var g errgroup.Group
			for i := 0; i < parallel; i++ {
				g.Go(func() error { return Eval(ctx, testExecutor{}, tasks, nil) })
			}

			var (
				const0   = tasks[0].Deps[0].Task(0)
				const1   = tasks[0].Deps[0].Task(1)
				cogroup0 = tasks[0]
				cogroup1 = tasks[1]
			)
			waitState(t, const0, TaskRunning)
			const0.Set(TaskOk)
			waitState(t, const1, TaskRunning)
			const1.Set(TaskOk)

			waitState(t, cogroup0, TaskRunning)
			waitState(t, cogroup1, TaskRunning)
			const0.Set(TaskLost)
			cogroup0.Set(TaskLost)
			cogroup1.Set(TaskLost)

			// Now, the evaluator must first recompute const0.
			waitState(t, const0, TaskRunning)
			// ... and then each of the cogroup tasks
			const0.Set(TaskOk)
			waitState(t, cogroup0, TaskRunning)
			waitState(t, cogroup1, TaskRunning)
			cogroup0.Set(TaskOk)
			cogroup1.Set(TaskOk)

			if err := g.Wait(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestPersistentTaskLoss verifies that the evaluator will abandon evaluation
// with a task that is repeatedly lost on attempts to run it, as it is unable to
// make meaningful progress.
func TestPersistentTaskLoss(t *testing.T) {
	var (
		test        simpleEvalTest
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	)
	defer cancel()
	test.Go(t)
	fst := test.ConstTask
	for {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		fst.Lock()
		for fst.state != TaskRunning {
			if err := fst.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
		fst.state = TaskLost
		fst.Broadcast()
		for fst.state == TaskLost {
			if err := fst.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
		isErr := fst.state == TaskErr
		fst.Unlock()
		if isErr {
			// The evaluator has given up on the task.
			break
		}
	}
	err := test.EvalErr()
	if !errors.Is(errors.TooManyTries, err) {
		t.Errorf("expected TooManyTries error, got: %v", err)
	}
}

func multiPhaseCompile(nshard, nstage int) []*Task {
	tasks, _, _ := compileFunc(func() bigslice.Slice {
		keys := make([]string, nshard*2)
		for i := range keys {
			keys[i] = fmt.Sprint(i)
		}
		values := make([]int, nshard*2)
		for i := range values {
			values[i] = i
		}

		slice := bigslice.Const(nshard, keys, values)
		for stage := 0; stage < nstage; stage++ {
			slice = bigslice.Reduce(slice, func(i, j int) int { return i + j })
		}
		return slice
	})
	return tasks
}

func TestMultiPhaseEval(t *testing.T) {
	const (
		S = 1000
		P = 10
	)
	tasks := multiPhaseCompile(S, P)
	if got, want := len(tasks), S; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	var phases [][]*Task
	for task := tasks[0].Deps[0].Task(0); ; {
		phases = append(phases, task.Group)
		if len(task.Deps) == 0 {
			break
		}
		task = task.Deps[0].Task(0)
	}
	if got, want := len(phases), P; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, group := range phases {
		if got, want := len(group), S; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	eval := func() (wait func()) {
		var g errgroup.Group
		g.Go(func() error {
			t.Helper()
			return Eval(context.Background(), testExecutor{}, tasks, nil)
		})
		return func() {
			t.Helper()
			if err := g.Wait(); err != nil {
				t.Fatal(err)
			}
		}
	}

	wait := eval()

	for i := len(phases) - 1; i >= 0; i-- {
		group := phases[i]
		for _, task := range group {
			waitState(t, task, TaskRunning)
		}
		// Make sure no other tasks are waiting or running.
		for j := i - 1; j >= 0; j-- {
			otherGroup := phases[j]
			for _, task := range otherGroup {
				if task.State() != TaskInit {
					t.Fatal(task, ": wrong state")
				}
			}
		}
		for _, task := range group {
			task.Set(TaskOk)
		}
	}

	for _, task := range tasks {
		waitState(t, task, TaskRunning)
		task.Set(TaskOk)
	}
	wait()

	mustState := func(task *Task, state TaskState) {
		t.Helper()
		if got, want := task.State(), state; got != want {
			t.Fatalf("%v: got %v, want %v", task, got, want)
		}
	}

	mustStates := func(def TaskState, states map[*Task]TaskState) {
		t.Helper()
		for _, group := range phases {
			for _, task := range group {
				state, ok := states[task]
				if !ok {
					state = def
				}
				mustState(task, state)
			}
		}
		for _, task := range tasks {
			state, ok := states[task]
			if !ok {
				state = def
			}
			mustState(task, state)
		}
	}

	// An exterior task failure means a single resubmit.
	tasks[S/2].Set(TaskLost)
	wait = eval()

	waitState(t, tasks[S/2], TaskRunning)
	mustStates(TaskOk, map[*Task]TaskState{
		tasks[S/2]: TaskRunning,
	})
	tasks[S/2].Set(TaskOk)
	wait()

	// A reachable path of interior task failures get resubmitted.
	lost := []*Task{
		tasks[S/2],
		phases[0][S/2],
		phases[1][S/2],
	}
	unreachable := phases[3][S/2]
	for _, task := range lost {
		task.Set(TaskLost)
	}
	unreachable.Set(TaskLost)
	wait = eval()
	waitState(t, lost[len(lost)-1], TaskRunning)
	mustStates(TaskOk, map[*Task]TaskState{
		unreachable: TaskLost,
		lost[0]:     TaskLost,
		lost[1]:     TaskLost,
		lost[2]:     TaskRunning,
	})
	lost[2].Set(TaskOk)
	waitState(t, lost[1], TaskRunning)
	mustStates(TaskOk, map[*Task]TaskState{
		unreachable: TaskLost,
		lost[0]:     TaskLost,
		lost[1]:     TaskRunning,
	})
	lost[1].Set(TaskOk)
	waitState(t, lost[0], TaskRunning)
	mustStates(TaskOk, map[*Task]TaskState{
		unreachable: TaskLost,
		lost[0]:     TaskRunning,
	})
	lost[0].Set(TaskOk)
	mustStates(TaskOk, map[*Task]TaskState{
		unreachable: TaskLost,
	})
	wait()
}

type benchExecutor struct{ *testing.B }

func (benchExecutor) Start(*Session) (shutdown func()) {
	return func() {}
}

func (b benchExecutor) Run(task *Task) {
	task.Lock()
	task.state = TaskOk
	task.Broadcast()
	task.Unlock()
}

func (benchExecutor) Reader(*Task, int) sliceio.ReadCloser {
	panic("not implemented")
}

func (benchExecutor) Discard(context.Context, *Task) {}

func (benchExecutor) Eventer() eventlog.Eventer {
	return eventlog.Nop{}
}

func (benchExecutor) HandleDebug(handler *http.ServeMux) {
	panic("not implemented")
}

var evalStages = flag.Int("eval.bench.stages", 5, "number of stages for eval benchmark")

func BenchmarkEval(b *testing.B) {
	for _, nshard := range []int{10, 100, 1000, 5000 /*, 100000*/} {
		b.Run(fmt.Sprintf("eval.%d", nshard), func(b *testing.B) {
			ctx := context.Background()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tasks := multiPhaseCompile(nshard, *evalStages)
				if i == 0 {
					b.Log("ntask=", len(tasks))
				}
				b.StartTimer()
				if err := Eval(ctx, benchExecutor{b}, tasks, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEnqueue(b *testing.B) {
	for _, nshard := range []int{10, 100, 1000, 5000 /*, 100000*/} {
		b.Run(fmt.Sprintf("enqueue.%d", nshard), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tasks := multiPhaseCompile(nshard, *evalStages)
				if i == 0 {
					b.Log("ntask=", len(tasks))
				}
				state := newState()
				b.StartTimer()

				for _, task := range tasks {
					state.Enqueue(task)
				}
			}
		})
	}
}
