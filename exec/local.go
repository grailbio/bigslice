// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"

	"github.com/grailbio/base/backgroundcontext"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/eventlog"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/metrics"
	"github.com/grailbio/bigslice/sliceio"
)

// LocalExecutor is an executor that runs tasks in-process in
// separate goroutines. All output is buffered in memory.
type localExecutor struct {
	mu      sync.Mutex
	state   map[*Task]TaskState
	buffers map[*Task]taskBuffer
	limiter *limiter.Limiter
	sess    *Session
}

func newLocalExecutor() *localExecutor {
	return &localExecutor{
		state:   make(map[*Task]TaskState),
		buffers: make(map[*Task]taskBuffer),
		limiter: limiter.New(),
	}
}

func (*localExecutor) Name() string {
	return "local"
}

func (l *localExecutor) Start(sess *Session) (shutdown func()) {
	l.sess = sess
	l.limiter.Release(sess.p)
	return
}

func (l *localExecutor) Run(task *Task) {
	ctx := backgroundcontext.Get()
	n := 1
	if task.Pragma.Exclusive() {
		n = l.sess.p
	}
	if err := l.limiter.Acquire(ctx, n); err != nil {
		// The only errors we should encounter here are context errors,
		// in which case there is no more work to do.
		if err != context.Canceled && err != context.DeadlineExceeded {
			log.Panicf("exec.Local: unexpected error: %v", err)
		}
		return
	}
	defer l.limiter.Release(n)
	in, err := l.depReaders(ctx, task)
	if err != nil {
		if errors.Match(fatalErr, err) {
			task.Error(err)
		} else {
			task.Set(TaskLost)
		}
		return
	}
	task.Set(TaskRunning)

	// Start execution, then place output in a task buffer. We also plumb a
	// metrics scope in here so we can store and aggregate metrics.
	task.Scope.Reset(nil)
	out := task.Do(in)
	buf, err := bufferOutput(metrics.ScopedContext(ctx, &task.Scope), task, out)
	task.Lock()
	if err == nil {
		l.mu.Lock()
		l.buffers[task] = buf
		l.mu.Unlock()
		task.state = TaskOk
	} else {
		if errors.Match(fatalErr, err) {
			task.state = TaskErr
		} else {
			task.state = TaskLost
		}
		task.err = err
	}
	task.Broadcast()
	task.Unlock()
}

func (l *localExecutor) depReaders(ctx context.Context, task *Task) ([]sliceio.Reader, error) {
	in := make([]sliceio.Reader, 0, len(task.Deps))
	for _, dep := range task.Deps {
		reader := new(multiReader)
		reader.q = make([]sliceio.Reader, dep.NumTask())
		for j := 0; j < dep.NumTask(); j++ {
			reader.q[j] = l.Reader(dep.Task(j), dep.Partition)
		}
		if dep.NumTask() > 0 && !dep.Task(0).Combiner.IsNil() {
			// Perform input combination in-line, one for each partition.
			combineKey := task.Name
			if task.CombineKey != "" {
				combineKey = TaskName{Op: task.CombineKey}
			}
			combiner, err := newCombiner(dep.Task(0), combineKey.String(), dep.Task(0).Combiner, *defaultChunksize*100)
			if err != nil {
				return nil, errors.E(errors.Fatal, "could not make combiner", err)
			}
			buf := frame.Make(dep.Task(0), *defaultChunksize, *defaultChunksize)
			for {
				n, err := reader.Read(ctx, buf)
				if err != nil && err != sliceio.EOF {
					return nil, err
				}
				if err := combiner.Combine(ctx, buf.Slice(0, n)); err != nil {
					return nil, err
				}
				if err == sliceio.EOF {
					break
				}
			}
			reader, err := combiner.Reader()
			if err != nil {
				return nil, err
			}
			in = append(in, reader)
		} else if dep.Expand {
			in = append(in, reader.q...)
		} else {
			in = append(in, reader)
		}
	}
	return in, nil
}

func (l *localExecutor) Reader(task *Task, partition int) sliceio.ReadCloser {
	l.mu.Lock()
	buf, ok := l.buffers[task]
	l.mu.Unlock()
	if !ok {
		return sliceio.ReaderWithCloseFunc{
			Reader:    sliceio.ErrReader(fmt.Errorf("no data for %v", task)),
			CloseFunc: func() error { return nil },
		}
	}
	return buf.Reader(partition)
}

func (l *localExecutor) Discard(_ context.Context, task *Task) {
	if !task.Combiner.IsNil() && task.CombineKey != "" {
		// We do not yet handle tasks with shared combiners.
		return
	}
	task.Lock()
	if task.state == TaskOk {
		l.mu.Lock()
		delete(l.buffers, task)
		l.mu.Unlock()
		task.Unlock()
		task.Set(TaskLost)
		return
	}
	task.Unlock()
}

func (l *localExecutor) Eventer() eventlog.Eventer {
	return l.sess.eventer
}

func (*localExecutor) HandleDebug(*http.ServeMux) {}

// BufferOutput reads the output from reader and places it in a
// task buffer. If the output is partitioned, bufferOutput invokes
// the task's partitioner in order to determine the correct partition.
func bufferOutput(ctx context.Context, task *Task, out sliceio.Reader) (buf taskBuffer, err error) {
	if task.NumOut() == 0 {
		_, err := out.Read(ctx, frame.Empty)
		if err == sliceio.EOF {
			err = nil
		}
		return nil, err
	}
	buf = make(taskBuffer, task.NumPartition)
	var in frame.Frame
	defer func() {
		if e := recover(); e != nil {
			stack := debug.Stack()
			err = fmt.Errorf("panic while evaluating slice: %v\n%s", e, string(stack))
			err = errors.E(err, errors.Fatal)
		}
	}()
	shards := make([]int, *defaultChunksize)
	for {
		if in.IsZero() {
			in = frame.Make(task, *defaultChunksize, *defaultChunksize)
		}
		n, err := out.Read(ctx, in)
		if err != nil && err != sliceio.EOF {
			return nil, err
		}
		// If the output needs to be partitioned, we ask the partitioner to
		// assign partitions to each input element, and then append the
		// elements in their respective partitions. In this case, we just
		// maintain buffer slices of defaultChunksize each.
		if task.NumPartition > 1 {
			task.Partitioner(ctx, in, task.NumPartition, shards[:n])
			for i := 0; i < n; i++ {
				p := shards[i]
				// If we don't yet have a buffer or the current one is at capacity,
				// create a new one.
				m := len(buf[p])
				if m == 0 || buf[p][m-1].Cap() == buf[p][m-1].Len() {
					frame := frame.Make(task, 0, *defaultChunksize)
					buf[p] = append(buf[p], frame)
					m++
				}
				buf[p][m-1] = frame.AppendFrame(buf[p][m-1], in.Slice(i, i+1))
			}
		} else if n > 0 {
			in = in.Slice(0, n)
			buf[0] = append(buf[0], in)
			in = frame.Frame{}
		}
		if err == sliceio.EOF {
			break
		}
	}
	return buf, nil
}

type multiReader struct {
	q   []sliceio.Reader
	err error
}

func (m *multiReader) Read(ctx context.Context, out frame.Frame) (n int, err error) {
	if m.err != nil {
		return 0, m.err
	}
	for len(m.q) > 0 {
		n, err := m.q[0].Read(ctx, out)
		switch {
		case err == sliceio.EOF:
			m.q = m.q[1:]
		case err != nil:
			m.err = err
			return n, err
		case n > 0:
			return n, err
		}
	}
	return 0, sliceio.EOF
}
