package store

import "time"

// Value is the minimal value contract required by the cache. Len is used for
// byte-based capacity accounting.
type Value interface {
	Len() int
}

type Store interface {
	Get(key string) (Value, bool)
	Set(key string, value Value) error
	SetWithExpiration(key string, value Value, expiration time.Duration) error
	Delete(key string) bool
	Clear()
	Len() int
	Close()
}

type CacheType string

const (
	// LRU is a single-lock LRU, mainly useful for small caches and tests.
	LRU CacheType = "lru"
	// ShardedLRU partitions keys across independent LRU shards so unrelated
	// keys do not contend on one global lock.
	ShardedLRU CacheType = "sharded-lru"
)

type Options struct {
	MaxBytes        int64
	ShardCount      int
	CleanupInterval time.Duration
	OnEvicted       func(key string, value Value)
}

func NewOptions() Options {
	return Options{
		MaxBytes:        8 << 20,
		ShardCount:      16,
		CleanupInterval: time.Minute,
	}
}

func NewStore(cacheType CacheType, opts Options) Store {
	switch cacheType {
	case LRU:
		return newLRUCache(opts)
	case ShardedLRU:
		return newShardedLRU(opts)
	default:
		return newShardedLRU(opts)
	}
}
