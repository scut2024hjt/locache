package store

import (
	"container/list"
	"sync"
	"time"
)

type lruCache struct {
	mu              sync.Mutex
	list            *list.List
	items           map[string]*list.Element
	expires         map[string]time.Time
	maxBytes        int64
	usedBytes       int64
	onEvicted       func(key string, value Value)
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{}
	closeOnce       sync.Once
}

type lruEntry struct {
	key   string
	value Value
}

func newLRUCache(opts Options) *lruCache {
	return newLRUCacheInternal(opts, true)
}

func newLRUCacheInternal(opts Options, startCleanup bool) *lruCache {
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}
	c := &lruCache{
		list:            list.New(),
		items:           make(map[string]*list.Element),
		expires:         make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		onEvicted:       opts.OnEvicted,
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
	}
	if startCleanup {
		c.cleanupTicker = time.NewTicker(cleanupInterval)
		go c.cleanupLoop()
	}
	return c
}

func (c *lruCache) Get(key string) (Value, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if exp, hasExp := c.expires[key]; hasExp && !time.Now().Before(exp) {
		c.removeElement(elem)
		return nil, false
	}
	c.list.MoveToBack(elem)
	return elem.Value.(*lruEntry).value, true
}

func (c *lruCache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, 0)
}

func (c *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if expiration > 0 {
		c.expires[key] = time.Now().Add(expiration)
	} else {
		delete(c.expires, key)
	}

	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*lruEntry)
		c.usedBytes += int64(value.Len() - entry.value.Len())
		entry.value = value
		c.list.MoveToBack(elem)
		c.evictLocked()
		return nil
	}

	entry := &lruEntry{key: key, value: value}
	elem := c.list.PushBack(entry)
	c.items[key] = elem
	c.usedBytes += int64(len(key) + value.Len())
	c.evictLocked()
	return nil
}

func (c *lruCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}

func (c *lruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.onEvicted != nil {
		for _, elem := range c.items {
			entry := elem.Value.(*lruEntry)
			c.onEvicted(entry.key, entry.value)
		}
	}
	c.list.Init()
	c.items = make(map[string]*list.Element)
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
}

func (c *lruCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.list.Len()
}

func (c *lruCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*lruEntry)
	c.list.Remove(elem)
	delete(c.items, entry.key)
	delete(c.expires, entry.key)
	c.usedBytes -= int64(len(entry.key) + entry.value.Len())
	if c.onEvicted != nil {
		c.onEvicted(entry.key, entry.value)
	}
}

func (c *lruCache) evictLocked() {
	now := time.Now()
	for key, exp := range c.expires {
		if !now.Before(exp) {
			if elem, ok := c.items[key]; ok {
				c.removeElement(elem)
			}
		}
	}
	for c.maxBytes > 0 && c.usedBytes > c.maxBytes && c.list.Len() > 0 {
		c.removeElement(c.list.Front())
	}
}

func (c *lruCache) cleanupExpired() {
	c.mu.Lock()
	c.evictLocked()
	c.mu.Unlock()
}

func (c *lruCache) cleanupLoop() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.cleanupExpired()
		case <-c.closeCh:
			return
		}
	}
}

func (c *lruCache) Close() {
	c.closeOnce.Do(func() {
		if c.cleanupTicker != nil {
			c.cleanupTicker.Stop()
		}
		close(c.closeCh)
	})
}

func (c *lruCache) GetWithExpiration(key string) (Value, time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return nil, 0, false
	}
	now := time.Now()
	if exp, hasExp := c.expires[key]; hasExp {
		if !now.Before(exp) {
			c.removeElement(elem)
			return nil, 0, false
		}
		c.list.MoveToBack(elem)
		return elem.Value.(*lruEntry).value, exp.Sub(now), true
	}
	c.list.MoveToBack(elem)
	return elem.Value.(*lruEntry).value, 0, true
}
