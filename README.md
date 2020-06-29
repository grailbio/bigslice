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
- [![CI](https://github.com/grailbio/bigslice/workflows/CI/badge.svg)](https://github.com/grailbio/bigslice/actions?query=workflow%3ACI) [![Full Test](https://github.com/grailbio/bigslice/workflows/Full%20Test/badge.svg)](https://github.com/grailbio/bigslice/actions?query=workflow%3A%22Full+Test%22)

# Developing Bigslice

Bigslice uses Go modules to capture its dependencies;
no tooling other than the base Go install is required.

```
$ git clone https://github.com/grailbio/bigslice
$ cd bigslice
$ GO111MODULE=on go test
```

If tests fail with `socket: too many open files` errors, try increasing the maximum number of open files by running
```
$ ulimit -n 2000
```
