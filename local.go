// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"net/http"
	"reflect"
	"runtime"
	"sync"
)

// LocalExecutor is an executor that runs tasks in-process in
// separate goroutines. All output is buffered in memory.
type localExecutor struct {
	mu      sync.Mutex
	buffers map[*Task]taskBuffer
	sess    *Session
}

func newLocalExecutor() *localExecutor {
	return &localExecutor{
		buffers: make(map[*Task]taskBuffer),
	}
}

// Maxprocs returns the maxprocs reported by the Go runtime.
func (localExecutor) Maxprocs() int { return runtime.GOMAXPROCS(0) }

func (l *localExecutor) Start(sess *Session) (shutdown func()) {
	l.sess = sess
	return
}

func (l *localExecutor) Run(ctx context.Context, inv Invocation, task *Task) error {
	// Note that we don't need to manage invocations here because we
	// have direct access to the tasks, which are already compiled for
	// us before the evaluator calls us. All previous task outputs are available
	// locally.
	in := make([]Reader, len(task.Deps))
	for i, dep := range task.Deps {
		reader := new(multiReader)
		reader.q = make([]Reader, len(dep.Tasks))
		for j, deptask := range dep.Tasks {
			reader.q[j] = l.Reader(ctx, deptask, dep.Partition)
		}
		in[i] = reader
	}

	// Start execution, then place output in a task buffer.
	out := task.Do(in)
	buf, err := bufferOutput(ctx, task, out)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.buffers[task] = buf
	l.mu.Unlock()
	return nil
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
	if len(task.Out) == 0 {
		_, err := out.Read(ctx)
		if err == EOF {
			err = nil
		}
		return nil, err
	}
	var (
		buf        = make(taskBuffer, task.NumPartition)
		in         []reflect.Value
		partitions []int
	)
	if task.NumPartition > 1 {
		partitions = make([]int, defaultChunksize, defaultChunksize)
	}
	for {
		if in == nil {
			in = make([]reflect.Value, len(task.Out))
			for i := range in {
				in[i] = reflect.MakeSlice(reflect.SliceOf(task.Out[i]), defaultChunksize, defaultChunksize)
			}
		}
		n, err := out.Read(ctx, in...)
		if err != nil && err != EOF {
			return nil, err
		}
		// If the output needs to be partitioned, we ask the partitioner to
		// assign partitions to each input element, and then append the
		// elements in their respective partitions. In this case, we just
		// maintain buffer slices of defaultChunksize each.
		if task.NumPartition > 1 {
			task.Partitioner.Partition(in, partitions, n, task.NumPartition)
			for i := 0; i < n; i++ {
				p := partitions[i]
				// If we don't yet have a vector or the current one is at capacity,
				// create a new one.
				m := len(buf[p])
				if m == 0 || buf[p][m-1][0].Cap() == buf[p][m-1][0].Len() {
					cols := make([]reflect.Value, len(task.Out))
					for j, typ := range task.Out {
						cols[j] = reflect.MakeSlice(reflect.SliceOf(typ), 0, defaultChunksize)
					}
					buf[p] = append(buf[p], cols)
					m++
				}
				for j := range buf[p][m-1] {
					buf[p][m-1][j] = reflect.Append(buf[p][m-1][j], in[j].Index(i))
				}
			}
		} else if n > 0 {
			for i := range in {
				in[i] = in[i].Slice(0, n)
			}
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

func (m *multiReader) Read(ctx context.Context, columns ...reflect.Value) (n int, err error) {
	if m.err != nil {
		return 0, m.err
	}
	for len(m.q) > 0 {
		n, err := m.q[0].Read(ctx, columns...)
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
