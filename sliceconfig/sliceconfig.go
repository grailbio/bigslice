// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package sliceconfig provides a mechanism to create a bigslice
// session from a shared configuration. Sliceconfig uses the
// configuration mechanism in package
// github.com/grailbio/base/config, and reads a default profile from
// $HOME/.bigslice/config. Configurations may be provisioned
// using the bigslice command.
package sliceconfig

import (
	"flag"
	"os"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/must"

	// Used to provide ec2system.System bigmachines.
	_ "github.com/grailbio/bigmachine/ec2system"
	"github.com/grailbio/bigslice/exec"
)

// Path determines the location of the bigslice profile read
// by Parse.
var Path = os.ExpandEnv("$HOME/.bigslice/config")

// Parse registers configuration flags, bigslice flags, and calls
// flag.Parse. It reads bigslice configuration from Path defined in
// this package. Parse returns session as configured by the
// configuration and any flags provided. Parse panics if session
// creation fails.
func Parse() (sess *exec.Session, shutdown func()) {
	config.RegisterFlags("", Path)
	flag.Parse()
	must.Nil(config.ProcessFlags())
	config.Must("bigslice", &sess)
	return sess, func() {}
}
