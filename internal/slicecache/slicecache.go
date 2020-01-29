package slicecache

import (
	"context"
	"fmt"
	"runtime"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/bigslice/sliceio"
)

// Cacheable indicates a slice's data should be cached.
type Cacheable interface {
	Cache() ShardCache
}

// ShardCache accesses cached data for a slice's shards.
type ShardCache interface {
	IsCached(shard int) bool
	WritethroughReader(shard int, reader sliceio.Reader) sliceio.Reader
	CacheReader(shard int) sliceio.Reader
}

// Empty is an empty cache.
var Empty = empty{}

type empty struct{}

func (empty) IsCached(shard int) bool { return false }
func (empty) WritethroughReader(shard int, reader sliceio.Reader) sliceio.Reader {
	return reader
}
func (empty) CacheReader(shard int) sliceio.Reader { panic("always empty") }

// FileShardCache is a ShardCache backed by files. A nil *FileShardCache has no
// cached data.
type FileShardCache struct {
	prefix        string
	numShards     int
	shardIsCached []bool
}

// NewShardCache constructs a ShardCache. It does O(numShards) parallelized
// file operations to look up what's present in the cache.
func NewFileShardCache(ctx context.Context, prefix string, numShards int) (*FileShardCache, error) {
	if prefix == "" {
		return &FileShardCache{}, nil
	}
	// TODO(jcharumilind): Make this initialization more lazy. This is generally
	// called within Funcs, but its result is generally ignored on workers to
	// ensure a consistent view of the cache for consistent compilation.
	c := FileShardCache{prefix, numShards, make([]bool, numShards)}
	_ = traverse.Limit(10*runtime.NumCPU()).Each(numShards, func(shard int) error {
		_, err := file.Stat(ctx, c.path(shard))
		c.shardIsCached[shard] = err == nil // treat lookup errors as cache misses
		return nil
	})
	return &c, nil
}

func (c *FileShardCache) path(shard int) string {
	return fmt.Sprintf("%s-%04d-of-%04d", c.prefix, shard, c.numShards)
}

func (c *FileShardCache) IsCached(shard int) bool {
	if c == nil {
		return false
	}
	return c.shardIsCached[shard]
}

func (c *FileShardCache) RequireAllCached() {
	if c == nil {
		return
	}
	for _, b := range c.shardIsCached {
		if !b {
			for i := range c.shardIsCached {
				c.shardIsCached[i] = false
			}
			return
		}
	}
}

// WritethroughReader returns a reader that populates the cache. reader should
// read computed data.
func (c *FileShardCache) WritethroughReader(shard int, reader sliceio.Reader) sliceio.Reader {
	if c == nil {
		return reader
	}
	return newWritethroughReader(reader, c.path(shard))
}

// CacheReader returns a reader that reads from the cache.
func (c *FileShardCache) CacheReader(shard int) sliceio.Reader {
	if !c.shardIsCached[shard] {
		log.Panicf("shard %d is not cached", shard)
	}
	return newFileReader(c.path(shard))
}
