// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
)

// Store is an abstraction that stores data named by task and partition.
type Store interface {
	// Create returns a writer that populates data for the given
	// task name and partition. The data is not be available
	// to Open until the returned closer has been closed.
	//
	// TODO(marius): should we allow writes to be discarded as well?
	Create(ctx context.Context, task string, partition int) (io.WriteCloser, error)

	// Open returns a ReadCloser from which the stored contents of the
	// named task and partition can be read. If the task and partition are
	// not stored, an error with kind errors.NotExist is returned.
	Open(ctx context.Context, task string, partition int) (io.ReadCloser, error)
}

// MemoryStore is a store implementation that maintains in-memory buffers
// of task output.
type memoryStore struct {
	mu    sync.Mutex
	tasks map[string][][]byte
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		tasks: make(map[string][][]byte),
	}
}

func (m *memoryStore) get(task string, partition int) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.tasks[task]) <= partition {
		return nil
	}
	return m.tasks[task][partition]
}

func (m *memoryStore) put(task string, partition int, p []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for len(m.tasks[task]) <= partition {
		m.tasks[task] = append(m.tasks[task], nil)
	}
	if m.tasks[task][partition] != nil {
		return errors.E(errors.Exists, "partition already stored")
	}
	m.tasks[task][partition] = p
	return nil
}

type memoryWriter struct {
	bytes.Buffer
	task      string
	partition int
	store     *memoryStore
}

func (m *memoryWriter) Close() error {
	return m.store.put(m.task, m.partition, m.Buffer.Bytes())
}

func (m *memoryStore) Create(ctx context.Context, task string, partition int) (io.WriteCloser, error) {
	if m.get(task, partition) != nil {
		return nil, errors.E(errors.Exists, fmt.Sprintf("create %s[%d]", task, partition))
	}
	return &memoryWriter{
		task:      task,
		partition: partition,
		store:     m,
	}, nil
}

func (m *memoryStore) Open(ctx context.Context, task string, partition int) (io.ReadCloser, error) {
	p := m.get(task, partition)
	if p == nil {
		return nil, errors.E(errors.NotExist, fmt.Sprintf("open %s[%d]", task, partition))
	}
	return ioutil.NopCloser(bytes.NewReader(p)), nil
}

// FileStore is a store implementation that uses grailfiles; thus
// task output can be stored at any URL supported by grailfile (e.g.,
// S3).
type fileStore struct {
	// Prefix is the grailfile prefix under which task data are stored.
	// A task's output is stored at "{Prefix}{task}p{partition}".
	Prefix string
}

func (s *fileStore) path(task string, partition int) string {
	return fmt.Sprintf("%s%sp%03d", s.Prefix, task, partition)
}

func (s *fileStore) Create(ctx context.Context, task string, partition int) (io.WriteCloser, error) {
	f, err := file.Create(ctx, s.path(task, partition))
	if err != nil {
		return nil, err
	}
	return &fileIOCloser{
		Writer: f.Writer(ctx),
		ctx:    ctx,
		file:   f,
	}, nil
}

func (s *fileStore) Open(ctx context.Context, task string, partition int) (io.ReadCloser, error) {
	f, err := file.Open(ctx, s.path(task, partition))
	if err != nil {
		return nil, err
	}
	return &fileIOCloser{
		Reader: f.Reader(ctx),
		ctx:    ctx,
		file:   f,
	}, nil
}

type fileIOCloser struct {
	io.Writer
	io.Reader
	ctx  context.Context
	file file.File
}

func (f *fileIOCloser) Close() error {
	return f.file.Close(f.ctx)
}
