// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package mapio

import (
	"bytes"
	"container/heap"
)

var scanSentinel = new(MapScanner)

// Merged represents the merged contents of multiple underlying maps.
// Like Map, Merged presents a sorted, scannable map, but it does not
// guarantee that the order of traversal is stable.
type Merged []*Map

// Seek returns a scanner for the merged map that starts at the first
// entry where entryKey <= key.
func (m Merged) Seek(key []byte) *MergedScanner {
	merged := make(MergedScanner, 0, len(m)+1)
	for i := range m {
		s := m[i].Seek(key)
		if !s.Scan() {
			if err := s.Err(); err != nil {
				return &MergedScanner{s}
			}
			// Otherwise it's just empty and we can skip it.
			continue
		}
		merged = append(merged, s)
	}
	if len(merged) == 0 {
		return &MergedScanner{}
	}
	heap.Init(&merged)
	merged = append(merged, scanSentinel)
	return &merged
}

// MergedScanner is a scanner for merged maps.
type MergedScanner []*MapScanner

// Len implements heap.Interface
func (m MergedScanner) Len() int { return len(m) }

// Less implements heap.Interface
func (m MergedScanner) Less(i, j int) bool { return bytes.Compare(m[i].Key(), m[j].Key()) < 0 }

// Swap implements heap.Interface
func (m MergedScanner) Swap(i, j int) { m[i], m[j] = m[j], m[i] }

// Push implements heap.Interface
func (m *MergedScanner) Push(x interface{}) {
	*m = append(*m, x.(*MapScanner))
}

// Pop implements heap.Interface
func (m *MergedScanner) Pop() interface{} {
	n := len(*m)
	elem := (*m)[n-1]
	*m = (*m)[:n-1]
	return elem
}

// Scan scans the next entry in the merged map, returning true on
// success. If Scan returns false, the caller should check Err to
// distinguish between scan completion and scan error.
func (m *MergedScanner) Scan() bool {
	if len(*m) == 0 || (*m)[0].err != nil {
		return false
	}
	if len(*m) > 0 && (*m)[len(*m)-1] == scanSentinel {
		*m = (*m)[:len(*m)-1]
		return true
	}

	if (*m)[0].Scan() {
		heap.Fix(m, 0)
	} else if (*m)[0].err == nil {
		heap.Remove(m, 0)
	}
	ok := len(*m) > 0 && (*m)[0].err == nil
	return ok

}

// Err returns the last error encountered while scanning, if any.
func (m MergedScanner) Err() error {
	if len(m) == 0 {
		return nil
	}
	return m[0].err
}

// Key returns the last key scanned.
func (m MergedScanner) Key() []byte {
	return m[0].Key()
}

// Value returns the last value scanned.
func (m MergedScanner) Value() []byte {
	return m[0].Value()
}
