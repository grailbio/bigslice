// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"sync"

	"github.com/grailbio/base/limiter"
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

// Maxprocs returns the maxprocs reported by the Go runtime.
func (localExecutor) Maxprocs() int { return runtime.GOMAXPROCS(0) }

func (l *localExecutor) Start(sess *Session) (shutdown func()) {
	l.sess = sess
	l.limiter.Release(sess.p)
	return
}

func (l *localExecutor) Runnable(task *Task) {
	task.Lock()
	defer task.Unlock()
	switch task.state {
	case TaskWaiting, TaskRunning:
		return
	}
	task.state = TaskWaiting
	task.Broadcast()

	go func() {
		ctx := context.Background()
		l.limiter.Acquire(ctx, 1)
		defer l.limiter.Release(1)
		in := make([]Reader, 0, len(task.Deps))
		for _, dep := range task.Deps {
			reader := new(multiReader)
			reader.q = make([]Reader, len(dep.Tasks))
			for j, deptask := range dep.Tasks {
				reader.q[j] = l.Reader(ctx, deptask, dep.Partition)
			}
			if dep.Expand {
				in = append(in, reader.q...)
			} else {
				in = append(in, reader)
			}
		}
		task.State(TaskRunning)
		// Start execution, then place output in a task buffer.
		out := task.Do(in)
		buf, err := bufferOutput(ctx, task, out)
		task.Lock()
		if err == nil {
			l.mu.Lock()
			l.buffers[task] = buf
			l.mu.Unlock()
			task.state = TaskOk
		} else {
			task.state = TaskErr
			task.err = err
		}
		task.Broadcast()
		task.Unlock()
	}()
}

func (l *localExecutor) Reader(_ context.Context, task *Task, partition int) Reader {
	l.mu.Lock()
	buf := l.buffers[task]
	l.mu.Unlock()
	return buf.Reader(partition)
}

func (*localExecutor) HandleDebug(*http.ServeMux) {}

// BufferOutput reads the output from reader and places it in a
// task buffer. If the output is partitioned, bufferOutput invokes
// the task's partitioner in order to determine the correct partition.
func bufferOutput(ctx context.Context, task *Task, out Reader) (taskBuffer, error) {
	if task.NumOut() == 0 {
		_, err := out.Read(ctx, nil)
		if err == EOF {
			err = nil
		}
		return nil, err
	}
	var (
		buf         = make(taskBuffer, task.NumPartition)
		in          Frame
		partitions  []int
		partitioner *partitioner
	)
	if task.Hasher != nil {
		partitions = make([]int, defaultChunksize, defaultChunksize)
		partitioner = newPartitioner(task.Hasher, task.NumPartition)
	} else if task.NumPartition != 1 {
		return nil, fmt.Errorf("invalid task graph: NumPartition is %d, but no Hasher provided", task.NumPartition)
	}
	for {
		if in == nil {
			in = MakeFrame(task, defaultChunksize)
		}
		n, err := out.Read(ctx, in)
		if err != nil && err != EOF {
			return nil, err
		}
		// If the output needs to be partitioned, we ask the partitioner to
		// assign partitions to each input element, and then append the
		// elements in their respective partitions. In this case, we just
		// maintain buffer slices of defaultChunksize each.
		if task.Hasher != nil {
			partitioner.Partition(in, partitions[:n])
			for i := 0; i < n; i++ {
				p := partitions[i]
				// If we don't yet have a buffer or the current one is at capacity,
				// create a new one.
				m := len(buf[p])
				if m == 0 || buf[p][m-1].Cap() == buf[p][m-1].Len() {
					frame := MakeFrame(task, 0, defaultChunksize)
					buf[p] = append(buf[p], frame)
					m++
				}
				for j := range buf[p][m-1] {
					buf[p][m-1][j] = reflect.Append(buf[p][m-1][j], in[j].Index(i))
				}
			}
		} else if n > 0 {
			in = in.Slice(0, n)
			buf[0] = append(buf[0], in)
			in = nil
		}
		if err == EOF {
			break
		}
	}
	return buf, nil
}

type multiReader struct {
	q   []Reader
	err error
}

func (m *multiReader) Read(ctx context.Context, out Frame) (n int, err error) {
	if m.err != nil {
		return 0, m.err
	}
	for len(m.q) > 0 {
		n, err := m.q[0].Read(ctx, out)
		switch {
		case err == EOF:
			err = nil
			m.q = m.q[1:]
		case err != nil:
			m.err = err
			return n, err
		case n > 0:
			return n, err
		}
	}
	return 0, EOF
}
