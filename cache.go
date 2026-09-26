package locache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/scut2024hjt/locache/store"
)

// Cache is the node-local storage wrapper. Thread safety is provided by the
// underlying Store implementation; counters here are atomic.
type Cache struct {
	store  store.Store
	hits   int64
	misses int64
	closed int32
}

type CacheOptions struct {
	CacheType   store.CacheType
	MaxBytes    int64
	ShardCount  int
	CleanupTime time.Duration
	OnEvicted   func(key string, value store.Value)
}

func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CacheType:   store.ShardedLRU,
		MaxBytes:    8 << 20,
		ShardCount:  16,
		CleanupTime: time.Minute,
	}
}

func NewCache(opts CacheOptions) *Cache {
	if opts.CacheType == "" {
		opts.CacheType = store.ShardedLRU
	}
	if opts.ShardCount <= 0 {
		opts.ShardCount = 16
	}
	if opts.CleanupTime <= 0 {
		opts.CleanupTime = time.Minute
	}
	return &Cache{store: store.NewStore(opts.CacheType, store.Options{
		MaxBytes:        opts.MaxBytes,
		ShardCount:      opts.ShardCount,
		CleanupInterval: opts.CleanupTime,
		OnEvicted:       opts.OnEvicted,
	})}
}

func (c *Cache) Add(key string, value ByteView) {
	if c == nil || atomic.LoadInt32(&c.closed) == 1 {
		return
	}
	_ = c.store.Set(key, value)
}

func (c *Cache) AddWithExpiration(key string, value ByteView, expirationTime time.Time) {
	if c == nil || atomic.LoadInt32(&c.closed) == 1 {
		return
	}
	ttl := time.Until(expirationTime)
	if ttl <= 0 {
		return
	}
	_ = c.store.SetWithExpiration(key, value, ttl)
}

func (c *Cache) Get(_ context.Context, key string) (ByteView, bool) {
	if c == nil || atomic.LoadInt32(&c.closed) == 1 {
		return ByteView{}, false
	}
	val, ok := c.store.Get(key)
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}
	view, ok := val.(ByteView)
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}
	atomic.AddInt64(&c.hits, 1)
	return view, true
}

func (c *Cache) Delete(key string) bool {
	if c == nil || atomic.LoadInt32(&c.closed) == 1 {
		return false
	}
	return c.store.Delete(key)
}

func (c *Cache) Clear() {
	if c == nil || atomic.LoadInt32(&c.closed) == 1 {
		return
	}
	c.store.Clear()
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

func (c *Cache) Len() int {
	if c == nil || atomic.LoadInt32(&c.closed) == 1 {
		return 0
	}
	return c.store.Len()
}

func (c *Cache) Close() {
	if c == nil || !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}
	c.store.Close()
}

func (c *Cache) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"closed": atomic.LoadInt32(&c.closed) == 1,
		"hits":   atomic.LoadInt64(&c.hits),
		"misses": atomic.LoadInt64(&c.misses),
		"size":   c.Len(),
	}
	total := stats["hits"].(int64) + stats["misses"].(int64)
	if total > 0 {
		stats["hit_rate"] = float64(stats["hits"].(int64)) / float64(total)
	} else {
		stats["hit_rate"] = 0.0
	}
	return stats
}
