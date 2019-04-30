// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package frame_test

import (
	"strings"
	"testing"

	"github.com/grailbio/bigslice/frame"
)

type testType struct{}

func TestDoubleRegistration(t *testing.T) {
	frame.RegisterOps(func(slice []testType) frame.Ops { return frame.Ops{} })
	message := func() (err interface{}) {
		defer func() {
			err = recover()
		}()
		frame.RegisterOps(func(slice []testType) frame.Ops { return frame.Ops{} })
		return nil
	}()
	if message == nil {
		t.Fatal("expected panic")
	}
	if message, ok := message.(string); ok {
		if !strings.HasPrefix(message, "frame.RegisterOps: ") || !strings.HasSuffix(message, "ops_test.go:17") {
			t.Errorf("wrong message %s", message)
		}
	} else {
		t.Errorf("wrong type %T for panic", message)
	}
}
