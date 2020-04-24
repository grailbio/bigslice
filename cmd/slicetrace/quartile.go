// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"time"
)

// computeQuartiles returns the quartiles of ds, using Tukey's method. q2 is the
// median of ds. q2 splits ds into two halves. q1 is the median of the lower
// half. q3 is the median of the upper half. If len(ds) is odd, q2 is included
// in the halves. ds must be non-empty.
func computeQuartiles(ds []time.Duration) (q1, q2, q3 time.Duration) {
	mid := len(ds) / 2
	q2 = computeMedian(ds)
	q3 = ds[(len(ds) - 1)]
	if len(ds) > 1 {
		q3 = computeMedian(ds[mid:])
	}
	var right int
	if len(ds)%2 == 0 {
		right = mid
	} else {
		right = mid + 1
	}
	q1 = computeMedian(ds[0:right])
	return
}

func computeMedian(ds []time.Duration) (d time.Duration) {
	mid := len(ds) / 2
	if len(ds)%2 == 0 {
		// Compute average without overflow.
		a, b := ds[mid-1], ds[mid]
		return (a / 2) + (b / 2) + (((a % 2) + (b % 2)) / 2)
	} else {
		return ds[mid]
	}
}
