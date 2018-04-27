// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package mapio

import (
	"errors"
	"io"
	"sync"
)

// Map is a read-only, sorted map backed by an io.ReadSeeker. The
// on-disk layout of maps are described by the package documentation.
// Maps support both lookup and (ordered) iteration. A Map instance
// maintains a current position, starting out at the first entry.
type Map struct {
	mu    sync.Mutex
	r     io.ReadSeeker
	index block
}

// New opens the map at the provided io.ReadSeeker (usually a file).
func New(r io.ReadSeeker) (*Map, error) {
	m := &Map{r: r}
	return m, m.init()
}

func (m *Map) init() error {
	if _, err := m.r.Seek(-mapTrailerSize, io.SeekEnd); err != nil {
		return err
	}
	trailer := make([]byte, mapTrailerSize)
	if _, err := io.ReadFull(m.r, trailer); err != nil {
		return err
	}
	metaAddr, _ := getBlockAddr(trailer)
	if metaAddr != (blockAddr{}) {
		return errors.New("non-empty meta block index")
	}
	indexAddr, _ := getBlockAddr(trailer[maxBlockAddrSize:])
	magic := order.Uint64(trailer[len(trailer)-8:])
	if magic != mapTrailerMagic {
		return errors.New("wrong magic")
	}
	if err := m.readBlock(indexAddr, &m.index); err != nil {
		return err
	}
	if !m.index.Scan() {
		return errors.New("empty index")
	}
	return nil
}

func (m *Map) readBlock(addr blockAddr, block *block) error {
	if block.p != nil && cap(block.p) >= int(addr.len) {
		block.p = block.p[:addr.len]
	} else {
		block.p = make([]byte, addr.len)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.r.Seek(int64(addr.off), io.SeekStart); err != nil {
		return err
	}
	if _, err := io.ReadFull(m.r, block.p); err != nil {
		return err
	}
	return block.init()
}

// Seek returns a map scanner beginning at the first key in the map
// >= the provided key.
func (m *Map) Seek(key []byte) *MapScanner {
	s := &MapScanner{parent: m, index: m.index}
	s.index.Seek(key)
	if s.index.Scan() {
		addr, _ := getBlockAddr(s.index.Value())
		if s.err = m.readBlock(addr, &s.data); s.err == nil {
			s.data.Seek(key)
		}
	}
	return s
}

// MapScanner implements ordered iteration over a map.
type MapScanner struct {
	parent      *Map
	err         error
	data, index block
}

// Scan scans the next entry, returning true on success. When Scan
// returns false, the caller should inspect Err to distinguish
// between scan completion and scan error.
func (m *MapScanner) Scan() bool {
	for m.err == nil && !m.data.Scan() {
		if !m.index.Scan() {
			return false
		}
		addr, _ := getBlockAddr(m.index.Value())
		m.err = m.parent.readBlock(addr, &m.data)
	}
	return m.err == nil
}

// Err returns the last error encountered while scanning.
func (m *MapScanner) Err() error {
	return m.err
}

// Key returns the key that was last scanned.
func (m *MapScanner) Key() []byte {
	return m.data.Key()
}

// Value returns the value that was last scanned.
func (m *MapScanner) Value() []byte {
	return m.data.Value()
}
