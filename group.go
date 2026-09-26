package locache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scut2024hjt/locache/singleflight"
	"github.com/sirupsen/logrus"
)

var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

var (
	ErrKeyRequired     = errors.New("key is required")
	ErrValueRequired   = errors.New("value is required")
	ErrGroupClosed     = errors.New("cache group is closed")
	ErrCacheMiss       = errors.New("cache miss")
	ErrNoOwner         = errors.New("no cache owner available")
	ErrPeerUnavailable = errors.New("cache owner is remote but no peer client is available")
	ErrTopologyChanged = errors.New("cache membership changed during operation")
	ErrNotOwner        = errors.New("this node is not the current owner")
)

// Getter optionally loads data from the source of truth when the owner cache
// misses. A Group may be created with a nil Getter and used as an explicit
// Get/Set/Delete distributed cache; with a Getter it additionally supports
// read-through cache filling.
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

type GetterFunc func(ctx context.Context, key string) ([]byte, error)

func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) { return f(ctx, key) }

// Group is a distributed cache namespace.
//
// A consistent-hash ring chooses one owner for every key. The owner's
// mainCache is the canonical cache copy. Non-owner nodes may optionally keep a
// short-TTL near-cache copy of remote reads. Near-cache entries are explicitly
// non-authoritative: Set/Delete first mutate the owner and then best-effort
// invalidate peer copies; TTL is the fallback bound if an invalidation is lost.
type Group struct {
	name       string
	getter     Getter
	mainCache  *Cache // canonical cache for keys owned by this node
	nearCache  *Cache // optional requester-side copies of remote reads
	nearTTL    time.Duration
	peers      PeerPicker
	loader     *singleflight.Group
	expiration time.Duration
	closed     int32
	stats      groupStats

	ownerEpochMu sync.Mutex
	ownerEpoch   uint64

	keyStatesMu sync.Mutex
	keyStates   map[string]*keyMutationState
}

type keyMutationState struct {
	mu         sync.Mutex
	generation uint64
	refs       int
}

type groupStats struct {
	localHits            int64
	localMisses          int64
	nearHits             int64
	nearMisses           int64
	peerHits             int64
	peerMisses           int64
	sourceLoads          int64
	sourceErrors         int64
	sourceDuration       int64
	sets                 int64
	deletes              int64
	invalidationFailures int64
}

type GroupOption func(*Group)

// WithExpiration sets the default owner-cache TTL for values written through
// Set or populated by the Getter. Zero means no automatic expiration.
func WithExpiration(d time.Duration) GroupOption {
	return func(g *Group) { g.expiration = d }
}

func WithPeers(peers PeerPicker) GroupOption {
	return func(g *Group) { g.peers = peers }
}

func WithCacheOptions(opts CacheOptions) GroupOption {
	return func(g *Group) { g.mainCache = NewCache(opts) }
}

// WithNearCache enables a requester-side cache for remote-owner reads.
//
// This is a latency/load optimization for read-heavy workloads that accept
// bounded staleness. Set/Delete perform best-effort peer invalidation, while
// nearTTL remains the correctness fallback when invalidation cannot reach a
// node. Set ttl=0 (or omit this option) when every read should reach the owner.
func WithNearCache(maxBytes int64, ttl time.Duration) GroupOption {
	return func(g *Group) {
		if maxBytes <= 0 || ttl <= 0 {
			return
		}
		opts := DefaultCacheOptions()
		opts.MaxBytes = maxBytes
		g.nearCache = NewCache(opts)
		g.nearTTL = ttl
	}
}

// WithHotCache is kept as an alias for callers of the previous API. Prefer
// WithNearCache in new code because it describes the role more precisely.
func WithHotCache(maxBytes int64, ttl time.Duration) GroupOption {
	return WithNearCache(maxBytes, ttl)
}

func NewGroup(name string, cacheBytes int64, getter Getter, opts ...GroupOption) *Group {
	g := &Group{
		name:      name,
		getter:    getter,
		loader:    &singleflight.Group{},
		keyStates: make(map[string]*keyMutationState),
	}
	for _, opt := range opts {
		opt(g)
	}
	if g.mainCache == nil {
		cacheOpts := DefaultCacheOptions()
		if cacheBytes > 0 {
			cacheOpts.MaxBytes = cacheBytes
		}
		g.mainCache = NewCache(cacheOpts)
	}

	groupsMu.Lock()
	old := groups[name]
	groups[name] = g
	groupsMu.Unlock()
	if old != nil && old != g {
		_ = old.Close()
	}
	logrus.Infof("created cache group [%s], ownerTTL=%v, nearTTL=%v, readThrough=%v", name, g.expiration, g.nearTTL, g.getter != nil)
	return g
}

func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

func (g *Group) waitForTopologyRetry(ctx context.Context, epoch uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if g.peers == nil {
		return nil
	}
	const maxWait = 75 * time.Millisecond
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		if g.peers.Epoch() != epoch {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-timer.C:
			// Avoid a tight retry loop when a remote owner has already observed a
			// newer membership view than this requester.
			return nil
		}
	}
}

// Get routes the key to its unique owner. Remote-owner results may be kept in
// the optional near-cache for nearTTL.
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}
	if key == "" {
		return ByteView{}, ErrKeyRequired
	}
	if g.peers == nil {
		return g.getLocal(ctx, key)
	}

	for attempt := 0; attempt < 4; attempt++ {
		owner, peer, isSelf, epoch, ok := g.peers.PickOwner(key)
		if !ok || owner == "" {
			return ByteView{}, ErrNoOwner
		}
		if isSelf {
			view, err := g.getLocal(ctx, key)
			if errors.Is(err, ErrTopologyChanged) || errors.Is(err, ErrNotOwner) {
				continue
			}
			return view, err
		}
		if peer == nil {
			return ByteView{}, fmt.Errorf("%w: owner=%s", ErrPeerUnavailable, owner)
		}

		nearKey := g.nearKey(epoch, key)
		if g.nearCache != nil {
			if view, hit := g.nearCache.Get(ctx, nearKey); hit {
				ownerNow, _, _, epochNow, okNow := g.peers.PickOwner(key)
				if okNow && ownerNow == owner && epochNow == epoch {
					atomic.AddInt64(&g.stats.nearHits, 1)
					return view, nil
				}
				g.nearCache.Delete(nearKey)
			}
			atomic.AddInt64(&g.stats.nearMisses, 1)
		}

		bytes, err := peer.Get(ctx, g.name, key)
		if err != nil {
			atomic.AddInt64(&g.stats.peerMisses, 1)
			if errors.Is(err, ErrNotOwner) || errors.Is(err, ErrTopologyChanged) {
				if attempt == 3 {
					return ByteView{}, ErrTopologyChanged
				}
				if waitErr := g.waitForTopologyRetry(ctx, epoch); waitErr != nil {
					return ByteView{}, waitErr
				}
				continue
			}
			return ByteView{}, fmt.Errorf("get %q from owner %s: %w", key, owner, err)
		}

		ownerNow, _, isSelfNow, epochNow, okNow := g.peers.PickOwner(key)
		if !okNow || ownerNow != owner || epochNow != epoch || isSelfNow {
			continue
		}

		atomic.AddInt64(&g.stats.peerHits, 1)
		view := ByteView{b: cloneBytes(bytes)}
		if g.nearCache != nil {
			g.nearCache.AddWithExpiration(nearKey, view, time.Now().Add(g.nearTTL))
		}
		return view, nil
	}
	return ByteView{}, ErrTopologyChanged
}

// Set writes the key to its unique owner. After the owner acknowledges the
// mutation, requester-side copies on other nodes are invalidated on a
// best-effort basis. A failed invalidation does not roll back the owner write;
// near-cache TTL is the fallback staleness bound.
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	owner := ""
	var stableEpoch uint64
	for attempt := 0; attempt < 4; attempt++ {
		if g.peers == nil {
			if err := g.setLocal(key, value); err != nil {
				return err
			}
			break
		}

		var peer Peer
		var isSelf, ok bool
		var epoch uint64
		owner, peer, isSelf, epoch, ok = g.peers.PickOwner(key)
		if !ok || owner == "" {
			return ErrNoOwner
		}
		if isSelf {
			if err := g.setLocal(key, value); err != nil {
				if errors.Is(err, ErrTopologyChanged) || errors.Is(err, ErrNotOwner) {
					continue
				}
				return err
			}
		} else {
			if peer == nil {
				return fmt.Errorf("%w: owner=%s", ErrPeerUnavailable, owner)
			}
			if err := peer.Set(ctx, g.name, key, value); err != nil {
				if errors.Is(err, ErrNotOwner) || errors.Is(err, ErrTopologyChanged) {
					if attempt == 3 {
						return ErrTopologyChanged
					}
					if waitErr := g.waitForTopologyRetry(ctx, epoch); waitErr != nil {
						return waitErr
					}
					continue
				}
				return fmt.Errorf("set %q on owner %s: %w", key, owner, err)
			}
		}

		ownerNow, _, _, epochNow, okNow := g.peers.PickOwner(key)
		if okNow && ownerNow == owner && epochNow == epoch {
			stableEpoch = epoch
			break
		}
		if attempt == 3 {
			return ErrTopologyChanged
		}
	}

	atomic.AddInt64(&g.stats.sets, 1)
	g.invalidateLocalCopies(key)
	g.invalidatePeerCopies(ctx, key, owner)

	if g.peers != nil && g.nearCache != nil && owner != "" {
		ownerNow, _, isSelfNow, epochNow, ok := g.peers.PickOwner(key)
		if ok && ownerNow == owner && epochNow == stableEpoch && !isSelfNow {
			g.nearCache.AddWithExpiration(g.nearKey(stableEpoch, key), ByteView{b: cloneBytes(value)}, time.Now().Add(g.nearTTL))
		}
	}
	return nil
}

// Delete evicts the key from the owner and then best-effort invalidates cached
// copies on other nodes. It does not mutate the external source of truth.
func (g *Group) Delete(ctx context.Context, key string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}

	owner := ""
	for attempt := 0; attempt < 4; attempt++ {
		if g.peers == nil {
			g.deleteLocal(key)
			break
		}

		var peer Peer
		var isSelf, ok bool
		var epoch uint64
		owner, peer, isSelf, epoch, ok = g.peers.PickOwner(key)
		if !ok || owner == "" {
			return ErrNoOwner
		}
		if isSelf {
			g.deleteLocal(key)
		} else {
			if peer == nil {
				return fmt.Errorf("%w: owner=%s", ErrPeerUnavailable, owner)
			}
			if _, err := peer.Delete(ctx, g.name, key); err != nil {
				if errors.Is(err, ErrNotOwner) || errors.Is(err, ErrTopologyChanged) {
					if attempt == 3 {
						return ErrTopologyChanged
					}
					if waitErr := g.waitForTopologyRetry(ctx, epoch); waitErr != nil {
						return waitErr
					}
					continue
				}
				return fmt.Errorf("delete %q on owner %s: %w", key, owner, err)
			}
		}

		ownerNow, _, _, epochNow, okNow := g.peers.PickOwner(key)
		if okNow && ownerNow == owner && epochNow == epoch {
			break
		}
		if attempt == 3 {
			return ErrTopologyChanged
		}
	}

	atomic.AddInt64(&g.stats.deletes, 1)
	g.invalidateLocalCopies(key)
	g.invalidatePeerCopies(ctx, key, owner)
	return nil
}

// getLocal is the owner-only read path. RPC handlers call it directly so a
// peer request cannot recursively enter distributed routing.
func (g *Group) getLocal(ctx context.Context, key string) (ByteView, error) {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}

	epoch, err := g.prepareLocalOwner(key)
	if err != nil {
		return ByteView{}, err
	}

	state := g.acquireKeyState(key)
	defer g.releaseKeyState(key, state)

	if view, ok := g.mainCache.Get(ctx, key); ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return view, nil
	}
	atomic.AddInt64(&g.stats.localMisses, 1)

	if g.getter == nil {
		return ByteView{}, ErrCacheMiss
	}

	loaded, err := g.loader.Do(key, func() (interface{}, error) {
		state.mu.Lock()
		if view, ok := g.mainCache.Get(ctx, key); ok {
			state.mu.Unlock()
			return view, nil
		}
		startGeneration := state.generation
		state.mu.Unlock()

		start := time.Now()
		atomic.AddInt64(&g.stats.sourceLoads, 1)
		bytes, loadErr := g.getter.Get(ctx, key)
		atomic.AddInt64(&g.stats.sourceDuration, time.Since(start).Nanoseconds())
		if loadErr != nil {
			atomic.AddInt64(&g.stats.sourceErrors, 1)
			return nil, loadErr
		}

		if g.peers != nil {
			_, _, isSelfNow, epochNow, ok := g.peers.PickOwner(key)
			if !ok || !isSelfNow || epochNow != epoch {
				return nil, ErrTopologyChanged
			}
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.generation != startGeneration {
			if view, ok := g.mainCache.Get(ctx, key); ok {
				return view, nil
			}
			return nil, ErrCacheMiss
		}

		view := ByteView{b: cloneBytes(bytes)}
		g.addOwnerValue(key, view)
		return view, nil
	})
	if err != nil {
		if errors.Is(err, ErrCacheMiss) || errors.Is(err, ErrTopologyChanged) || errors.Is(err, ErrNotOwner) {
			return ByteView{}, err
		}
		return ByteView{}, fmt.Errorf("load %q from source: %w", key, err)
	}
	return loaded.(ByteView), nil
}

func (g *Group) setLocal(key string, value []byte) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if _, err := g.prepareLocalOwner(key); err != nil && g.peers != nil {
		return err
	}
	state := g.acquireKeyState(key)
	defer g.releaseKeyState(key, state)

	state.mu.Lock()
	state.generation++
	g.addOwnerValue(key, ByteView{b: cloneBytes(value)})
	state.mu.Unlock()

	g.invalidateNearKey(key)
	return nil
}

func (g *Group) addOwnerValue(key string, view ByteView) {
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}
}

func (g *Group) deleteOwnerLocal(key string) (bool, error) {
	if atomic.LoadInt32(&g.closed) == 1 {
		return false, ErrGroupClosed
	}
	if _, err := g.prepareLocalOwner(key); err != nil && g.peers != nil {
		return false, err
	}
	return g.deleteLocal(key), nil
}

func (g *Group) deleteLocal(key string) bool {
	state := g.acquireKeyState(key)
	defer g.releaseKeyState(key, state)

	state.mu.Lock()
	state.generation++
	removed := g.mainCache.Delete(key)
	state.mu.Unlock()

	g.invalidateNearKey(key)
	return removed
}

func (g *Group) invalidateNearKey(key string) {
	if g.nearCache == nil {
		return
	}
	if g.peers != nil {
		g.nearCache.Delete(g.nearKey(g.peers.Epoch(), key))
	}
}

func (g *Group) invalidateLocalCopies(key string) {
	// mainCache may contain a stale copy from a previous ownership epoch.
	if g.peers != nil {
		owner, _, isSelf, _, ok := g.peers.PickOwner(key)
		if ok && owner != "" && !isSelf {
			g.mainCache.Delete(key)
		}
	}
	g.invalidateNearKey(key)
}

func (g *Group) prepareLocalOwner(key string) (uint64, error) {
	if g.peers == nil {
		return 0, nil
	}
	owner, _, isSelf, epoch, ok := g.peers.PickOwner(key)
	if !ok || owner == "" {
		return 0, ErrNoOwner
	}
	if !isSelf {
		return 0, fmt.Errorf("%w: owner=%s", ErrNotOwner, owner)
	}
	g.ownerEpochMu.Lock()
	if g.ownerEpoch == 0 {
		g.ownerEpoch = epoch
	} else if g.ownerEpoch != epoch {
		// Membership changes can move ownership away and later back. Old
		// owner-cache entries must never become authoritative again.
		g.mainCache.Clear()
		g.ownerEpoch = epoch
	}
	g.ownerEpochMu.Unlock()
	return epoch, nil
}

func (g *Group) acquireKeyState(key string) *keyMutationState {
	g.keyStatesMu.Lock()
	state := g.keyStates[key]
	if state == nil {
		state = &keyMutationState{}
		g.keyStates[key] = state
	}
	state.refs++
	g.keyStatesMu.Unlock()
	return state
}

func (g *Group) releaseKeyState(key string, state *keyMutationState) {
	g.keyStatesMu.Lock()
	state.refs--
	if state.refs == 0 && g.keyStates[key] == state {
		delete(g.keyStates, key)
	}
	g.keyStatesMu.Unlock()
}

func (g *Group) invalidatePeerCopies(ctx context.Context, key, owner string) {
	if g.peers == nil {
		return
	}
	if err := g.peers.Invalidate(ctx, g.name, key, owner); err != nil {
		atomic.AddInt64(&g.stats.invalidationFailures, 1)
		logrus.Warnf("best-effort near-cache invalidation failed for group=%s key=%s: %v", g.name, key, err)
	}
}

func (g *Group) nearKey(epoch uint64, key string) string {
	return strconv.FormatUint(epoch, 10) + ":" + key
}

func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
}

func (g *Group) Clear() {
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}
	g.mainCache.Clear()
	if g.nearCache != nil {
		g.nearCache.Clear()
	}
}

func (g *Group) Close() error {
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}
	if g.mainCache != nil {
		g.mainCache.Close()
	}
	if g.nearCache != nil {
		g.nearCache.Close()
	}

	groupsMu.Lock()
	if groups[g.name] == g {
		delete(groups, g.name)
	}
	groupsMu.Unlock()
	return nil
}

func (g *Group) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"name":                  g.name,
		"closed":                atomic.LoadInt32(&g.closed) == 1,
		"owner_ttl":             g.expiration,
		"near_ttl":              g.nearTTL,
		"read_through":          g.getter != nil,
		"local_hits":            atomic.LoadInt64(&g.stats.localHits),
		"local_misses":          atomic.LoadInt64(&g.stats.localMisses),
		"near_hits":             atomic.LoadInt64(&g.stats.nearHits),
		"near_misses":           atomic.LoadInt64(&g.stats.nearMisses),
		"peer_hits":             atomic.LoadInt64(&g.stats.peerHits),
		"peer_misses":           atomic.LoadInt64(&g.stats.peerMisses),
		"source_loads":          atomic.LoadInt64(&g.stats.sourceLoads),
		"source_errors":         atomic.LoadInt64(&g.stats.sourceErrors),
		"sets":                  atomic.LoadInt64(&g.stats.sets),
		"deletes":               atomic.LoadInt64(&g.stats.deletes),
		"invalidation_failures": atomic.LoadInt64(&g.stats.invalidationFailures),
	}
	loads := stats["source_loads"].(int64)
	if loads > 0 {
		stats["avg_source_load_ms"] = float64(atomic.LoadInt64(&g.stats.sourceDuration)) / float64(loads) / float64(time.Millisecond)
	}
	stats["owner_cache"] = g.mainCache.Stats()
	if g.nearCache != nil {
		stats["near_cache"] = g.nearCache.Stats()
	}
	return stats
}

func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	return names
}

func DestroyGroup(name string) bool {
	groupsMu.RLock()
	g := groups[name]
	groupsMu.RUnlock()
	if g == nil {
		return false
	}
	_ = g.Close()
	return true
}

func DestroyAllGroups() {
	groupsMu.RLock()
	snapshot := make([]*Group, 0, len(groups))
	for _, g := range groups {
		snapshot = append(snapshot, g)
	}
	groupsMu.RUnlock()
	for _, g := range snapshot {
		_ = g.Close()
	}
}
