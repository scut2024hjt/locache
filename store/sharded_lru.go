package store

import (
	"hash/fnv"
	"sync"
	"time"
)

type shardedLRU struct {
	shards      []*lruCache
	cleanupTick *time.Ticker
	closeCh     chan struct{}
	closeOnce   sync.Once
}

func newShardedLRU(opts Options) *shardedLRU {
	count := opts.ShardCount
	if count <= 0 {
		count = 16
	}
	count = nextPowerOfTwo(count)
	// Very small total budgets should not be split into many unusably tiny
	// shards. Keep at least 1 KiB of budget per shard when a limit is set.
	if opts.MaxBytes > 0 {
		for count > 1 && opts.MaxBytes/int64(count) < 1024 {
			count >>= 1
		}
	}

	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}

	base, remainder := int64(0), int64(0)
	if opts.MaxBytes > 0 {
		base = opts.MaxBytes / int64(count)
		remainder = opts.MaxBytes % int64(count)
	}

	s := &shardedLRU{
		shards:      make([]*lruCache, count),
		cleanupTick: time.NewTicker(cleanupInterval),
		closeCh:     make(chan struct{}),
	}
	for i := 0; i < count; i++ {
		shardOpts := opts
		if opts.MaxBytes > 0 {
			shardOpts.MaxBytes = base
			if int64(i) < remainder {
				shardOpts.MaxBytes++
			}
		}
		// The sharded store owns one cleanup ticker for all shards; individual
		// LRU shards do not spawn background goroutines.
		s.shards[i] = newLRUCacheInternal(shardOpts, false)
	}
	go s.cleanupLoop()
	return s
}

func (s *shardedLRU) shard(key string) *lruCache {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return s.shards[int(h.Sum32())&(len(s.shards)-1)]
}

func (s *shardedLRU) Get(key string) (Value, bool)      { return s.shard(key).Get(key) }
func (s *shardedLRU) Set(key string, value Value) error { return s.shard(key).Set(key, value) }
func (s *shardedLRU) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	return s.shard(key).SetWithExpiration(key, value, expiration)
}
func (s *shardedLRU) Delete(key string) bool { return s.shard(key).Delete(key) }

func (s *shardedLRU) Clear() {
	for _, shard := range s.shards {
		shard.Clear()
	}
}

func (s *shardedLRU) Len() int {
	total := 0
	for _, shard := range s.shards {
		total += shard.Len()
	}
	return total
}

func (s *shardedLRU) cleanupLoop() {
	for {
		select {
		case <-s.cleanupTick.C:
			for _, shard := range s.shards {
				shard.cleanupExpired()
			}
		case <-s.closeCh:
			return
		}
	}
}

func (s *shardedLRU) Close() {
	s.closeOnce.Do(func() {
		s.cleanupTick.Stop()
		close(s.closeCh)
		for _, shard := range s.shards {
			shard.Close()
		}
	})
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
