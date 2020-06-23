# Bigslice

Bigslice is a serverless cluster data processing system for [Go](https://golang.org).
Bigslice exposes composable API
that lets the user express
data processing tasks in terms of a series of
data transformations that invoke user code.
The Bigslice runtime then
transparently parallelizes and distributes the work,
using the [Bigmachine](https://github.com/grailbio/bigmachine)
library to create an ad hoc cluster on a cloud provider.

- website: [bigslice.io](https://bigslice.io/)
- API documentation: [godoc.org/github.com/grailbio/bigslice](https://godoc.org/github.com/grailbio/bigslice)
- issue tracker: [github.com/grailbio/bigslice/issues](https://github.com/grailbio/bigslice/issues)
- Travis CI: [![Travis CI](https://travis-ci.org/grailbio/bigslice.svg) https://travis-ci.org/grailbio/bigslice](https://travis-ci.org/grailbio/bigslice)

# Developing Bigslice

Bigslice uses Go modules to capture its dependencies;
no tooling other than the base Go install is required.

```
$ git clone https://github.com/grailbio/bigslice
$ cd bigslice
$ GO111MODULE=on go test
```

If you experience errors running tests, and see `too many open files` in the logs, you can increase the maximum number of open files by running
```
ulimit -n 2000
```
