package slicecache

import (
	"context"
	"fmt"
	"runtime"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/bigslice/sliceio"
)

// Cacheable indicates a slice's data should be cached.
type Cacheable interface {
	Cache() *ShardCache
}

// ShardCache accesses cached data for a slice's shards.
// A nil *ShardCache has no cached data.
type ShardCache struct {
	prefix        string
	numShards     int
	shardIsCached []bool
}

// NewShardCache constructs a ShardCache. It does O(numShards) parallelized
// file operations to look up what's present in the cache.
func NewShardCache(ctx context.Context, prefix string, numShards int) (*ShardCache, error) {
	if prefix == "" {
		return &ShardCache{}, nil
	}
	c := ShardCache{prefix, numShards, make([]bool, numShards)}
	_ = traverse.Limit(10*runtime.NumCPU()).Each(numShards, func(shard int) error {
		_, err := file.Stat(ctx, c.path(shard))
		c.shardIsCached[shard] = err == nil // treat lookup errors as cache misses
		return nil
	})
	return &c, nil
}

func (c *ShardCache) path(shard int) string {
	return fmt.Sprintf("%s-%04d-of-%04d", c.prefix, shard, c.numShards)
}

func (c *ShardCache) IsCached(shard int) bool {
	if c == nil {
		return false
	}
	return c.shardIsCached[shard]
}

func (c *ShardCache) RequireAllCached() {
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

// Reader returns a reader that uses the cache, or populates it.
// reader should read computed data
func (c *ShardCache) Reader(shard int, reader sliceio.Reader) sliceio.Reader {
	if c == nil {
		return reader
	}
	if c.shardIsCached[shard] {
		return newFileReader(c.path(shard))
	}
	return newWritethroughReader(reader, c.path(shard))
}
