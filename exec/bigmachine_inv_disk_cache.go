package exec

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"

	"github.com/grailbio/base/compress/zstd"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/fileio"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
)

type invDiskCache struct {
	mu       sync.Mutex
	cacheDir string
	// invPaths maps invocation indices to cache files on disk.
	invPaths map[uint64]string
}

func newInvDiskCache() *invDiskCache {
	return &invDiskCache{invPaths: make(map[uint64]string)}
}

// REQUIRES: Caller holds c.mu.
func (c *invDiskCache) init() error {
	if c.cacheDir != "" {
		return nil
	}
	cacheDir, err := ioutil.TempDir("", "bigslice-inv-cache")
	if err != nil {
		return errors.E(err, "bigslice: could not create invocation disk cache")
	}
	c.cacheDir = cacheDir
	return nil
}

func (c *invDiskCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	must.Truef(c.invPaths != nil, "multiple close")

	if err := os.RemoveAll(c.cacheDir); err != nil {
		log.Printf("WARNING: error discarding bigslice invocation disk cache: %v", err)
	}
	c.invPaths = nil
}

func (c *invDiskCache) getOrCreate(invIndex uint64, create func(io.Writer) error) (io.ReadCloser, error) {
	c.mu.Lock() // Note: cache access is serialized.
	defer c.mu.Unlock()

	must.Truef(c.invPaths != nil, "call after close")
	if err := c.init(); err != nil {
		return nil, err
	}

	if _, ok := c.invPaths[invIndex]; !ok {
		err := func() (err error) {
			invPath := path.Join(c.cacheDir, strconv.Itoa(int(invIndex)))
			f, err := os.OpenFile(invPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
			if err != nil {
				return err
			}
			defer fileio.CloseAndReport(f, &err)
			zw, err := zstd.NewWriter(f)
			if err != nil {
				return err
			}
			defer fileio.CloseAndReport(zw, &err)
			if err = create(zw); err != nil {
				return err
			}
			c.invPaths[invIndex] = invPath
			return nil
		}()
		if err != nil {
			return nil, errors.E(err, "bigslice: could not create invocation disk cache entry")
		}
	}
	f, err := os.Open(c.invPaths[invIndex])
	if err != nil {
		return nil, errors.E(err, "bigslice: could not open invocation disk cache entry")
	}
	zr, err := zstd.NewReader(f)
	if err != nil {
		err = errors.E(err, "bigslice: could not open (zstd) invocation disk cache entry")
		fileio.CloseAndReport(f, &err)
		return nil, err
	}
	return fileReadCloser{ReadCloser: zr, file: f}, nil
}

type fileReadCloser struct {
	io.ReadCloser
	file *os.File
}

func (f fileReadCloser) Close() (err error) {
	fileio.CloseAndReport(f.ReadCloser, &err)
	fileio.CloseAndReport(f.file, &err)
	return
}
