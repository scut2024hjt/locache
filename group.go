package locache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/scut2024hjt/locache/singleflight"
)

var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

// ErrKeyRequired 键不能为空错误
var ErrKeyRequired = errors.New("key is required")

// ErrValueRequired 值不能为空错误
var ErrValueRequired = errors.New("value is required")

// ErrGroupClosed 组已关闭错误
var ErrGroupClosed = errors.New("cache group is closed")

// Getter 加载键值的回调函数接口
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc 函数类型实现 Getter 接口
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get 实现 Getter 接口
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

// Group 是一个缓存命名空间
type Group struct {
	name       string
	getter     Getter
	mainCache  *Cache
	peers      PeerPicker
	loader     *singleflight.Group
	expiration time.Duration // 缓存过期时间，0表示永不过期
	closed     int32         // 原子变量，标记组是否已关闭
	stats      groupStats    // 统计信息
}

// groupStats 保存组的统计信息
type groupStats struct {
	loads        int64 // 加载次数
	localHits    int64 // 本地缓存命中次数
	localMisses  int64 // 本地缓存未命中次数
	peerHits     int64 // 从对等节点获取成功次数
	peerMisses   int64 // 从对等节点获取失败次数
	loaderHits   int64 // 从加载器获取成功次数
	loaderErrors int64 // 从加载器获取失败次数
	loadDuration int64 // 加载总耗时（纳秒）
}

// GroupOption 定义Group的配置选项
type GroupOption func(*Group)

// WithExpiration 设置缓存过期时间
func WithExpiration(d time.Duration) GroupOption {
	return func(g *Group) {
		g.expiration = d
	}
}

// WithPeers 设置分布式节点
func WithPeers(peers PeerPicker) GroupOption {
	return func(g *Group) {
		g.peers = peers
	}
}

// WithCacheOptions 设置缓存选项
func WithCacheOptions(opts CacheOptions) GroupOption {
	return func(g *Group) {
		g.mainCache = NewCache(opts)
	}
}

// NewGroup 创建一个新的 Group 实例
func NewGroup(name string, cacheBytes int64, getter Getter, opts ...GroupOption) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	// 创建默认缓存选项
	cacheOpts := DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes

	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: NewCache(cacheOpts),
		loader:    &singleflight.Group{},
	}

	// 应用选项
	for _, opt := range opts {
		opt(g)
	}

	// 注册到全局组映射
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if _, exists := groups[name]; exists {
		logrus.Warnf("Group with name %s already exists, will be replaced", name)
	}

	groups[name] = g
	logrus.Infof("Created cache group [%s] with cacheBytes=%d, expiration=%v", name, cacheBytes, g.expiration)

	return g
}

// GetGroup 获取指定名称的组
func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

// Get 从缓存获取数据
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}

	if key == "" {
		return ByteView{}, ErrKeyRequired
	}

	// 从本地缓存获取
	view, ok := g.mainCache.Get(ctx, key)
	if ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return view, nil
	}

	atomic.AddInt64(&g.stats.localMisses, 1)

	// 尝试从其他节点获取或加载
	return g.load(ctx, key)
}

// Set 设置缓存值
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	// 检查是否是从其他节点同步过来的请求
	isPeerRequest := ctx.Value("from_peer") != nil

	// 创建缓存视图
	view := ByteView{b: cloneBytes(value)}

	// 设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}

	// 如果不是从其他节点同步过来的请求，且启用了分布式模式，同步到其他节点
	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "set", key, value)
	}

	return nil
}

// Delete 删除缓存值
func (g *Group) Delete(ctx context.Context, key string) error {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}

	// 从本地缓存删除
	g.mainCache.Delete(key)

	// 检查是否是从其他节点同步过来的请求
	isPeerRequest := ctx.Value("from_peer") != nil

	// 如果不是从其他节点同步过来的请求，且启用了分布式模式，同步到其他节点
	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "delete", key, nil)
	}

	return nil
}

// syncToPeers 同步操作到其他节点
func (g *Group) syncToPeers(ctx context.Context, op string, key string, value []byte) {
	if g.peers == nil {
		return
	}

	// 选择对等节点
	peer, ok, isSelf := g.peers.PickPeer(key)
	if !ok || isSelf {
		return
	}

	// 创建同步请求上下文
	syncCtx := context.WithValue(context.Background(), "from_peer", true)

	var err error
	switch op {
	case "set":
		err = peer.Set(syncCtx, g.name, key, value)
	case "delete":
		_, err = peer.Delete(g.name, key)
	}

	if err != nil {
		logrus.Errorf("[Locache] failed to sync %s to peer: %v", op, err)
	}
}

// Clear 清空缓存
func (g *Group) Clear() {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}

	g.mainCache.Clear()
	logrus.Infof("[Locache] cleared cache for group [%s]", g.name)
}

// Close 关闭组并释放资源
func (g *Group) Close() error {
	// 如果已经关闭，直接返回
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}

	// 关闭本地缓存
	if g.mainCache != nil {
		g.mainCache.Close()
	}

	// 从全局组映射中移除
	groupsMu.Lock()
	delete(groups, g.name)
	groupsMu.Unlock()

	logrus.Infof("[Locache] closed cache group [%s]", g.name)
	return nil
}

// load 加载数据
func (g *Group) load(ctx context.Context, key string) (value ByteView, err error) {
	// 使用 singleflight 确保并发请求只加载一次
	startTime := time.Now()
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		return g.loadData(ctx, key)
	})

	// 记录加载时间
	loadDuration := time.Since(startTime).Nanoseconds()
	atomic.AddInt64(&g.stats.loadDuration, loadDuration)
	atomic.AddInt64(&g.stats.loads, 1)

	if err != nil {
		atomic.AddInt64(&g.stats.loaderErrors, 1)
		return ByteView{}, err
	}

	view := viewi.(ByteView)

	// 设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}

	return view, nil
}

// loadData 实际加载数据的方法
func (g *Group) loadData(ctx context.Context, key string) (value ByteView, err error) {
	// 尝试从远程节点获取
	if g.peers != nil {
		peer, ok, isSelf := g.peers.PickPeer(key)
		if ok && !isSelf {
			value, err := g.getFromPeer(ctx, peer, key)
			if err == nil {
				atomic.AddInt64(&g.stats.peerHits, 1)
				return value, nil
			}

			atomic.AddInt64(&g.stats.peerMisses, 1)
			logrus.Warnf("[Locache] failed to get from peer: %v", err)
		}
	}

	// 从数据源加载
	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("failed to get data: %w", err)
	}

	atomic.AddInt64(&g.stats.loaderHits, 1)
	return ByteView{b: cloneBytes(bytes)}, nil
}

// getFromPeer 从其他节点获取数据
func (g *Group) getFromPeer(ctx context.Context, peer Peer, key string) (ByteView, error) {
	bytes, err := peer.Get(g.name, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("failed to get from peer: %w", err)
	}
	return ByteView{b: bytes}, nil
}

// RegisterPeers 注册PeerPicker
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
	logrus.Infof("[Locache] registered peers for group [%s]", g.name)
}

// Stats 返回缓存统计信息
func (g *Group) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"name":          g.name,
		"closed":        atomic.LoadInt32(&g.closed) == 1,
		"expiration":    g.expiration,
		"loads":         atomic.LoadInt64(&g.stats.loads),
		"local_hits":    atomic.LoadInt64(&g.stats.localHits),
		"local_misses":  atomic.LoadInt64(&g.stats.localMisses),
		"peer_hits":     atomic.LoadInt64(&g.stats.peerHits),
		"peer_misses":   atomic.LoadInt64(&g.stats.peerMisses),
		"loader_hits":   atomic.LoadInt64(&g.stats.loaderHits),
		"loader_errors": atomic.LoadInt64(&g.stats.loaderErrors),
	}

	// 计算各种命中率
	totalGets := stats["local_hits"].(int64) + stats["local_misses"].(int64)
	if totalGets > 0 {
		stats["hit_rate"] = float64(stats["local_hits"].(int64)) / float64(totalGets)
	}

	totalLoads := stats["loads"].(int64)
	if totalLoads > 0 {
		stats["avg_load_time_ms"] = float64(atomic.LoadInt64(&g.stats.loadDuration)) / float64(totalLoads) / float64(time.Millisecond)
	}

	// 添加缓存大小
	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}

	return stats
}

// ListGroups 返回所有缓存组的名称
func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}

	return names
}

// DestroyGroup 销毁指定名称的缓存组
func DestroyGroup(name string) bool {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if g, exists := groups[name]; exists {
		g.Close()
		delete(groups, name)
		logrus.Infof("[Locache] destroyed cache group [%s]", name)
		return true
	}

	return false
}

// DestroyAllGroups 销毁所有缓存组
func DestroyAllGroups() {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	for name, g := range groups {
		g.Close()
		delete(groups, name)
		logrus.Infof("[Locache] destroyed cache group [%s]", name)
	}
}
