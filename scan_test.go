// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/grailbio/bigslice"
)

func TestScanReader(t *testing.T) {
	const (
		N      = 1000
		Nshard = 13
	)
	var b bytes.Buffer
	for i := 0; i < N; i++ {
		fmt.Fprint(&b, i, "\n")
	}
	slice := bigslice.ScanReader(Nshard, func() (io.ReadCloser, error) {
		return ioutil.NopCloser(bytes.NewReader(b.Bytes())), nil
	})
	slice = bigslice.Map(slice, func(s string) (struct{}, int) {
		i, _ := strconv.ParseInt(s, 10, 64)
		return struct{}{}, int(i)
	})
	slice = bigslice.Reduce(slice, func(a, e int) int { return a + e })
	slice = bigslice.Map(slice, func(k struct{}, v int) int { return v })
	assertEqual(t, slice, false, []int{499500})
}
