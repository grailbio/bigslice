// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"testing"
	"time"

	"github.com/grailbio/bigmachine/testsystem"
)

func TestBigmachineExecutor(t *testing.T) {
	testFunc := Func(func() Slice {
		time.Sleep(500 * time.Millisecond)
		return Const(1, []int{})
	})

	sys := testsystem.New()
	x := newBigmachineExecutor(sys)
	defer x.Start(&Session{
		Context: context.Background(),
		p:       2,
	})()

	inv := testFunc.Invocation()
	slice := inv.Invoke()
	tasks, err := compile(make(taskNamer), inv, slice)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(tasks), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Make sure we don't hang if we have a timeout while invoking
	// and compiling the slice remotely.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = x.Run(ctx, tasks[0])
	cancel()
	if got, want := err, context.DeadlineExceeded; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Otherwise, it should be run again to completion.
	if err := x.Run(context.Background(), tasks[0]); err != nil {
		t.Fatal(err)
	}
}
