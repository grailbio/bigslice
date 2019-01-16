// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"strconv"
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
)

// sliceInfo stores metadata for a stored slice.
type sliceInfo struct {
	// Size is the raw, encoded byte size of the stored slice.
	// A value of -1 indicates the size is unknown.
	Size int64
	// Records contains the number of records in the stored slice.
	// A value of -1 indicates the number of records is unknown.
	Records int64
}

// A writeCommitter represents a committable write stream into a store.
type writeCommitter interface {
	io.Writer
	// Commit commits the written data to storage. The caller should
	// provide the number of records written as metadata.
	Commit(ctx context.Context, records int64) error
	// Discard discards the writer; it will not be committed.
	Discard(ctx context.Context) error
}

// Store is an abstraction that stores partitioned data as produced by a task.
type Store interface {
	// Create returns a writer that populates data for the given
	// task name and partition. The data is not be available
	// to Open until the returned closer has been closed.
	//
	// TODO(marius): should we allow writes to be discarded as well?
	Create(ctx context.Context, task TaskName, partition int) (writeCommitter, error)

	// Open returns a ReadCloser from which the stored contents of the named task
	// and partition can be read. If the task and partition are not stored, an
	// error with kind errors.NotExist is returned. The offset specifies the byte
	// position from which to read.
	Open(ctx context.Context, task TaskName, partition int, offset int64) (io.ReadCloser, error)

	// Stat returns metadata for the stored slice.
	Stat(ctx context.Context, task TaskName, partition int) (sliceInfo, error)
}

// MemoryStore is a store implementation that maintains in-memory buffers
// of task output.
type memoryStore struct {
	mu     sync.Mutex
	tasks  map[TaskName][][]byte
	counts map[TaskName][]int64
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		tasks:  make(map[TaskName][][]byte),
		counts: make(map[TaskName][]int64),
	}
}

func (m *memoryStore) get(task TaskName, partition int) ([]byte, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.tasks[task]) <= partition {
		return nil, 0
	}
	return m.tasks[task][partition], m.counts[task][partition]
}

func (m *memoryStore) put(task TaskName, partition int, p []byte, count int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for len(m.tasks[task]) <= partition {
		m.tasks[task] = append(m.tasks[task], nil)
		m.counts[task] = append(m.counts[task], 0)
	}
	if m.tasks[task][partition] != nil {
		return errors.E(errors.Exists, "partition already stored")
	}
	if p == nil {
		p = []byte{}
	}
	m.tasks[task][partition] = p
	m.counts[task][partition] = count
	return nil
}

type memoryWriter struct {
	bytes.Buffer
	task      TaskName
	partition int
	store     *memoryStore
}

func (*memoryWriter) Discard(context.Context) error {
	return nil
}

func (m *memoryWriter) Commit(ctx context.Context, count int64) error {
	return m.store.put(m.task, m.partition, m.Buffer.Bytes(), count)
}

func (m *memoryStore) Create(ctx context.Context, task TaskName, partition int) (writeCommitter, error) {
	if b, _ := m.get(task, partition); b != nil {
		return nil, errors.E(errors.Exists, fmt.Sprintf("create %s[%d]", task, partition))
	}
	return &memoryWriter{
		task:      task,
		partition: partition,
		store:     m,
	}, nil
}

func (m *memoryStore) Open(ctx context.Context, task TaskName, partition int, offset int64) (io.ReadCloser, error) {
	p, _ := m.get(task, partition)
	if p == nil {
		return nil, errors.E(errors.NotExist, fmt.Sprintf("open %s[%d]", task, partition))
	}
	if int64(len(p)) < offset {
		return nil, errors.E(errors.Invalid, fmt.Sprintf("open %s[%d]: seeked to %d, data size %d", task, partition, offset, len(p)))
	}
	return ioutil.NopCloser(bytes.NewReader(p[offset:])), nil
}

func (m *memoryStore) Stat(ctx context.Context, task TaskName, partition int) (sliceInfo, error) {
	b, n := m.get(task, partition)
	if b == nil {
		return sliceInfo{}, errors.E(errors.NotExist, fmt.Sprintf("stat %s[%d]", task, partition))
	}
	return sliceInfo{
		Size:    int64(len(b)),
		Records: n,
	}, nil
}

// FileStore is a store implementation that uses grailfiles; thus
// task output can be stored at any URL supported by grailfile (e.g.,
// S3).
type fileStore struct {
	// Prefix is the grailfile prefix under which task data are stored.
	// A task's output is stored at "{Prefix}/{ophash}/{op}/{shardspec}/p{partition}".
	Prefix string
}

func (s *fileStore) path(task TaskName, partition int) string {
	h := fnv.New32a()
	h.Write([]byte(task.String()))
	h0 := int64(h.Sum(nil)[0])
	path := file.Join(s.Prefix, strconv.FormatInt(h0, 16), task.Op)
	if task.IsCombiner() {
		path = file.Join(path, "combiner")
	} else {
		path = file.Join(path, fmt.Sprintf("%03d-of-%03d", task.Shard, task.NumShard))
	}
	return file.Join(path, fmt.Sprintf("p%03d", partition))
}

type fileWriter struct {
	file.File
	io.Writer
}

func (w *fileWriter) Commit(ctx context.Context, count int64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(count))
	if _, err := w.Write(b[:]); err != nil {
		return nil
	}
	return closeFile(ctx, w.File)
}

func (s *fileStore) Create(ctx context.Context, task TaskName, partition int) (writeCommitter, error) {
	path := s.path(task, partition)
	f, err := file.Create(ctx, path)
	if err != nil {
		return nil, err
	}
	return &fileWriter{File: f, Writer: f.Writer(ctx)}, nil
}

func (s *fileStore) Open(ctx context.Context, task TaskName, partition int, offset int64) (io.ReadCloser, error) {
	f, err := file.Open(ctx, s.path(task, partition))
	if err != nil {
		return nil, err
	}
	info, err := f.Stat(ctx)
	if err != nil {
		return nil, err
	}
	r := f.Reader(ctx)
	if n, err := r.Seek(offset, io.SeekStart); err != nil || n != offset {
		if err == nil {
			return nil, errors.E(errors.Invalid, fmt.Sprintf("Seeked to %d, got %d", offset, n))
		}
	}
	return &fileIOCloser{
		Reader: io.LimitReader(r, info.Size()-8-offset),
		ctx:    ctx,
		file:   f,
	}, nil
}

func (s *fileStore) Stat(ctx context.Context, task TaskName, partition int) (sliceInfo, error) {
	f, err := file.Open(ctx, s.path(task, partition))
	if err != nil {
		return sliceInfo{}, err
	}
	rs := f.Reader(ctx)
	n, err := rs.Seek(-8, io.SeekEnd)
	if err != nil {
		return sliceInfo{}, err
	}
	var b [8]byte
	if _, err := rs.Read(b[:]); err != nil {
		return sliceInfo{}, err
	}
	count := int64(binary.LittleEndian.Uint64(b[:]))
	return sliceInfo{
		Size:    n,
		Records: count,
	}, nil
}

type fileIOCloser struct {
	io.Writer
	io.Reader
	ctx  context.Context
	file file.File
}

func (f *fileIOCloser) Close() error {
	return closeFile(f.ctx, f.file)
}

type closeNoSyncer interface {
	CloseNoSync(context.Context) error
}

// CloseFile closes the provided file. It avoids syncing if the implementation
// supports it.
func closeFile(ctx context.Context, f file.File) error {
	if closer, ok := f.(closeNoSyncer); ok {
		return closer.CloseNoSync(ctx)
	}
	return f.Close(ctx)
}
