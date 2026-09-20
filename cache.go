package locache

import (
	"context"
	"github.com/scut2024hjt/locache/store"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// Cache 是对底层缓存存储的封装
type Cache struct {
	mu          sync.RWMutex
	store       store.Store  // 底层存储实现
	opts        CacheOptions // 缓存配置选项
	hits        int64        // 缓存命中次数
	misses      int64        // 缓存未命中次数
	initialized int32        // 原子变量，标记缓存是否已初始化
	closed      int32        // 原子变量，标记缓存是否已关闭
}

// CacheOptions 缓存配置选项
type CacheOptions struct {
	CacheType    store.CacheType                     // 缓存类型: LRU, LRU2 等
	MaxBytes     int64                               // 最大内存使用量
	BucketCount  uint16                              // 缓存桶数量 (用于 LRU2)
	CapPerBucket uint16                              // 每个缓存桶的容量 (用于 LRU2)
	Level2Cap    uint16                              // 二级缓存桶的容量 (用于 LRU2)
	CleanupTime  time.Duration                       // 清理间隔
	OnEvicted    func(key string, value store.Value) // 驱逐回调
}

// DefaultCacheOptions 返回默认的缓存配置
func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CacheType:    store.LRU2,
		MaxBytes:     8 * 1024 * 1024, // 8MB
		BucketCount:  16,
		CapPerBucket: 512,
		Level2Cap:    256,
		CleanupTime:  time.Minute,
		OnEvicted:    nil,
	}
}

// NewCache 创建一个新的缓存实例
func NewCache(opts CacheOptions) *Cache {
	return &Cache{
		opts: opts,
	}
}

// ensureInitialized 确保缓存已初始化
func (c *Cache) ensureInitialized() {
	// 快速检查缓存是否已初始化，避免不必要的锁争用
	if atomic.LoadInt32(&c.initialized) == 1 {
		return
	}

	// 双重检查锁定模式
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.initialized == 0 {
		// 创建存储选项
		storeOpts := store.Options{
			MaxBytes:        c.opts.MaxBytes,
			BucketCount:     c.opts.BucketCount,
			CapPerBucket:    c.opts.CapPerBucket,
			Level2Cap:       c.opts.Level2Cap,
			CleanupInterval: c.opts.CleanupTime,
			OnEvicted:       c.opts.OnEvicted,
		}

		// 创建存储实例
		c.store = store.NewStore(c.opts.CacheType, storeOpts)

		// 标记为已初始化
		atomic.StoreInt32(&c.initialized, 1)

		logrus.Infof("Cache initialized with type %s, max bytes: %d", c.opts.CacheType, c.opts.MaxBytes)
	}
}

// Add 向缓存中添加一个 key-value 对
func (c *Cache) Add(key string, value ByteView) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	c.ensureInitialized()

	if err := c.store.Set(key, value); err != nil {
		logrus.Warnf("Failed to add key %s to cache: %v", key, err)
	}
}

// Get 从缓存中获取值
func (c *Cache) Get(ctx context.Context, key string) (value ByteView, ok bool) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return ByteView{}, false
	}

	// 如果缓存未初始化，直接返回未命中
	if atomic.LoadInt32(&c.initialized) == 0 {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// 从底层存储获取
	val, found := c.store.Get(key)
	if !found {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	// 更新命中计数
	atomic.AddInt64(&c.hits, 1)

	// 转换并返回
	if bv, ok := val.(ByteView); ok {
		return bv, true
	}

	// 类型断言失败
	logrus.Warnf("Type assertion failed for key %s, expected ByteView", key)
	atomic.AddInt64(&c.misses, 1)
	return ByteView{}, false
}

// AddWithExpiration 向缓存中添加一个带过期时间的 key-value 对
func (c *Cache) AddWithExpiration(key string, value ByteView, expirationTime time.Time) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	c.ensureInitialized()

	// 计算过期时间
	expiration := time.Until(expirationTime)
	if expiration <= 0 {
		logrus.Debugf("Key %s already expired, not adding to cache", key)
		return
	}

	// 设置到底层存储
	if err := c.store.SetWithExpiration(key, value, expiration); err != nil {
		logrus.Warnf("Failed to add key %s to cache with expiration: %v", key, err)
	}
}

// Delete 从缓存中删除一个 key
func (c *Cache) Delete(key string) bool {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.Delete(key)
}

// Clear 清空缓存
func (c *Cache) Clear() {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Clear()

	// 重置统计信息
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// Len 返回缓存的当前存储项数量
func (c *Cache) Len() int {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.Len()
}

// Close 关闭缓存，释放资源
func (c *Cache) Close() {
	// 如果已经关闭，直接返回
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 关闭底层存储
	if c.store != nil {
		if closer, ok := c.store.(interface{ Close() }); ok {
			closer.Close()
		}
		c.store = nil
	}

	// 重置缓存状态
	atomic.StoreInt32(&c.initialized, 0)

	logrus.Debugf("Cache closed, hits: %d, misses: %d", atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses))
}

// Stats 返回缓存统计信息
func (c *Cache) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"initialized": atomic.LoadInt32(&c.initialized) == 1,
		"closed":      atomic.LoadInt32(&c.closed) == 1,
		"hits":        atomic.LoadInt64(&c.hits),
		"misses":      atomic.LoadInt64(&c.misses),
	}

	if atomic.LoadInt32(&c.initialized) == 1 {
		stats["size"] = c.Len()

		// 计算命中率
		totalRequests := stats["hits"].(int64) + stats["misses"].(int64)
		if totalRequests > 0 {
			stats["hit_rate"] = float64(stats["hits"].(int64)) / float64(totalRequests)
		} else {
			stats["hit_rate"] = 0.0
		}
	}

	return stats
}
