// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Command sliceprofile demos how to use GRAIL profiles to configure
// bigslice.
package main

import (
	"context"
	"fmt"
	"log"

	// bigmachine/ec2system instance
	"github.com/grailbio/base/config"
	_ "github.com/grailbio/base/config/aws"
	"github.com/grailbio/base/grail"
	_ "github.com/grailbio/bigmachine"
	_ "github.com/grailbio/bigmachine/ec2system"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
)

var testFunc = bigslice.Func(func() (slice bigslice.Slice) {
	slice = bigslice.Const(1, []int{1, 2, 3})
	return
})

func main() {
	shutdown := grail.Init()
	defer shutdown()

	var sess *exec.Session
	config.Must("bigslice", &sess)
	ctx := context.Background()
	_, err := sess.Run(ctx, testFunc)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok")
}
