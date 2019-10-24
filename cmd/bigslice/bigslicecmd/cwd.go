package bigslicecmd

import "os"

var cwd string

// Init initializes the bigslicecmd package.
func Init() (err error) {
	cwd, err = os.Getwd()
	return
}
