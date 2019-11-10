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
	"net/http"

	// Imported to install pprof http handlers.
	_ "net/http/pprof"
	"os"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigmachine"

	// Imported to provide ec2system.System bigmachines.
	_ "github.com/grailbio/base/config/aws"
	_ "github.com/grailbio/bigmachine/ec2system"

	"github.com/grailbio/bigslice/exec"

	// Imported to provide an http server.
	_ "github.com/grailbio/base/config/http"
)

// Path determines the location of the bigslice profile read
// by Parse.
var Path = os.ExpandEnv("$HOME/.bigslice/config")

// Parse registers configuration flags, bigslice flags, and calls
// flag.Parse. It reads bigslice configuration from Path defined in
// this package. Parse returns session as configured by the
// configuration and any flags provided. Parse panics if session
// creation fails. Parse also instantiates the default http server
// according to the configuration profile, and registers the bigslice
// session status handlers with it.
func Parse() (sess *exec.Session, shutdown func()) {
	log.AddFlags()
	local := flag.Bool("local", false, "run bigslice in local mode")
	config.RegisterFlags("", Path)
	flag.Parse()
	must.Nil(config.ProcessFlags())
	if *local {
		sess = exec.Start(exec.Local, exec.Status(new(status.Status)))
	} else {
		bigmachine.Init()
		config.Must("bigslice", &sess)
	}
	sess.HandleDebug(http.DefaultServeMux)
	http.Handle("/debug/status", status.Handler(sess.Status()))
	config.Must("http", nil)
	return sess, func() {}
}
