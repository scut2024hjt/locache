package locache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scut2024hjt/locache/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakePeer struct {
	value []byte
	err   error
	calls atomic.Int64
}

func (p *fakePeer) Get(_ context.Context, _, _ string) ([]byte, error) {
	p.calls.Add(1)
	if p.err != nil {
		return nil, p.err
	}
	return cloneBytes(p.value), nil
}
func (p *fakePeer) Set(_ context.Context, _, _ string, value []byte) error {
	p.calls.Add(1)
	if p.err != nil {
		return p.err
	}
	p.value = cloneBytes(value)
	return nil
}
func (p *fakePeer) Delete(_ context.Context, _, _ string) (bool, error) {
	p.calls.Add(1)
	if p.err != nil {
		return false, p.err
	}
	p.value = nil
	return true, nil
}
func (p *fakePeer) Close() error { return nil }

type fakePicker struct {
	owner string
	self  string
	peer  Peer
	epoch atomic.Uint64
}

func newFakePicker(owner, self string, peer Peer) *fakePicker {
	p := &fakePicker{owner: owner, self: self, peer: peer}
	p.epoch.Store(1)
	return p
}
func (p *fakePicker) PickOwner(string) (string, Peer, bool, uint64, bool) {
	epoch := p.epoch.Load()
	if p.owner == "" {
		return "", nil, false, epoch, false
	}
	if p.owner == p.self {
		return p.owner, nil, true, epoch, true
	}
	return p.owner, p.peer, false, epoch, true
}

func (p *fakePicker) setOwner(owner string) {
	p.owner = owner
	p.epoch.Add(1)
}
func (p *fakePicker) Epoch() uint64                                            { return p.epoch.Load() }
func (p *fakePicker) Invalidate(context.Context, string, string, string) error { return nil }
func (p *fakePicker) Close() error                                             { return nil }

func cleanupGroups(t *testing.T) {
	t.Helper()
	DestroyAllGroups()
	t.Cleanup(DestroyAllGroups)
}

func TestOwnerLocalPathUsesSourceAndCaches(t *testing.T) {
	cleanupGroups(t)
	var loads atomic.Int64
	picker := newFakePicker("self", "self", nil)
	g := NewGroup("local", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		loads.Add(1)
		return []byte("value"), nil
	}), WithPeers(picker), WithExpiration(time.Minute))

	for i := 0; i < 2; i++ {
		v, err := g.Get(context.Background(), "k")
		if err != nil || v.String() != "value" {
			t.Fatalf("get=%q err=%v", v.String(), err)
		}
	}
	if loads.Load() != 1 {
		t.Fatalf("source loads=%d, want 1", loads.Load())
	}
}

func TestRemoteOwnerUsesHotCacheWithoutPollutingOwnerCache(t *testing.T) {
	cleanupGroups(t)
	peer := &fakePeer{value: []byte("remote")}
	picker := newFakePicker("remote", "self", peer)
	g := NewGroup("remote", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		return nil, errors.New("requester getter must not run")
	}), WithPeers(picker), WithHotCache(64<<10, time.Minute))

	for i := 0; i < 2; i++ {
		v, err := g.Get(context.Background(), "k")
		if err != nil || v.String() != "remote" {
			t.Fatalf("get=%q err=%v", v.String(), err)
		}
	}
	if peer.calls.Load() != 1 {
		t.Fatalf("peer calls=%d, want 1 due to hot cache", peer.calls.Load())
	}
	if got := g.mainCache.Len(); got != 0 {
		t.Fatalf("requester owner cache contains remote copy: %d entries", got)
	}
	if got := g.nearCache.Len(); got != 1 {
		t.Fatalf("hot cache entries=%d, want 1", got)
	}
}

func TestMembershipEpochInvalidatesHotCopy(t *testing.T) {
	cleanupGroups(t)
	peer := &fakePeer{value: []byte("v1")}
	picker := newFakePicker("remote", "self", peer)
	g := NewGroup("epoch", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		return nil, errors.New("unexpected local getter")
	}), WithPeers(picker), WithHotCache(64<<10, time.Minute))

	_, err := g.Get(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	peer.value = []byte("v2")
	picker.epoch.Add(1)
	v, err := g.Get(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "v2" {
		t.Fatalf("got %q after epoch change, want v2", v.String())
	}
	if peer.calls.Load() != 2 {
		t.Fatalf("peer calls=%d, want 2", peer.calls.Load())
	}
}

func TestSameKeyConcurrentMissLoadsSourceOnce(t *testing.T) {
	cleanupGroups(t)
	var loads atomic.Int64
	g := NewGroup("sf", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		loads.Add(1)
		time.Sleep(20 * time.Millisecond)
		return []byte("value"), nil
	}), WithExpiration(time.Minute))

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := g.Get(context.Background(), "hot-key")
			if err != nil || v.String() != "value" {
				t.Errorf("get=%q err=%v", v.String(), err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if loads.Load() != 1 {
		t.Fatalf("source loads=%d, want 1", loads.Load())
	}
}

func TestRemoteFailureDoesNotFallbackToRequesterSource(t *testing.T) {
	cleanupGroups(t)
	peer := &fakePeer{err: errors.New("owner unavailable")}
	picker := newFakePicker("remote", "self", peer)
	var loads atomic.Int64
	g := NewGroup("no-fallback", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		loads.Add(1)
		return []byte("wrong"), nil
	}), WithPeers(picker))

	if _, err := g.Get(context.Background(), "k"); err == nil {
		t.Fatal("expected remote-owner error")
	}
	if loads.Load() != 0 {
		t.Fatalf("requester source called %d times", loads.Load())
	}
}

func TestDestroyGroupDoesNotDeadlock(t *testing.T) {
	cleanupGroups(t)
	_ = NewGroup("destroy", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		return []byte("v"), nil
	}))

	done := make(chan bool, 1)
	go func() { done <- DestroyGroup("destroy") }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("group not destroyed")
		}
	case <-time.After(time.Second):
		t.Fatal("DestroyGroup deadlocked")
	}
}

func TestHotCacheProvidesBoundedStaleness(t *testing.T) {
	cleanupGroups(t)
	peer := &fakePeer{value: []byte("v1")}
	picker := newFakePicker("remote", "self", peer)
	g := NewGroup("bounded-stale", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		return nil, errors.New("unexpected local getter")
	}), WithPeers(picker), WithHotCache(64<<10, 25*time.Millisecond))

	first, err := g.Get(context.Background(), "k")
	if err != nil || first.String() != "v1" {
		t.Fatalf("first=%q err=%v", first.String(), err)
	}
	peer.value = []byte("v2")
	withinTTL, err := g.Get(context.Background(), "k")
	if err != nil || withinTTL.String() != "v1" {
		t.Fatalf("within TTL=%q err=%v, want cached v1", withinTTL.String(), err)
	}
	time.Sleep(35 * time.Millisecond)
	afterTTL, err := g.Get(context.Background(), "k")
	if err != nil || afterTTL.String() != "v2" {
		t.Fatalf("after TTL=%q err=%v, want refreshed v2", afterTTL.String(), err)
	}
}

func TestServerGetUsesOwnerLocalPathWithoutRerouting(t *testing.T) {
	cleanupGroups(t)
	peer := &fakePeer{value: []byte("wrong-peer")}
	// v3 起 owner-local 路径会校验"本节点是不是当前 Owner"，所以这里必须让
	// picker 认为本节点就是 Owner；非 Owner 拒服由下一个用例覆盖。
	picker := newFakePicker("self", "self", peer)
	g := NewGroup("server-local", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		return []byte("owner-source"), nil
	}), WithPeers(picker), WithExpiration(time.Minute))
	_ = g

	s := &Server{}
	resp, err := s.Get(context.Background(), &pb.Request{Group: "server-local", Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Value) != "owner-source" {
		t.Fatalf("server returned %q", string(resp.Value))
	}
	if peer.calls.Load() != 0 {
		t.Fatalf("server rerouted owner request %d times", peer.calls.Load())
	}
}

type ownerBackedPeer struct {
	owner *Group
	calls atomic.Int64
}

func (p *ownerBackedPeer) Get(ctx context.Context, _ string, key string) ([]byte, error) {
	p.calls.Add(1)
	v, err := p.owner.getLocal(ctx, key)
	if err != nil {
		return nil, err
	}
	return v.ByteSlice(), nil
}
func (p *ownerBackedPeer) Set(_ context.Context, _ string, key string, value []byte) error {
	p.calls.Add(1)
	return p.owner.setLocal(key, value)
}
func (p *ownerBackedPeer) Delete(_ context.Context, _ string, key string) (bool, error) {
	p.calls.Add(1)
	return p.owner.deleteLocal(key), nil
}
func (p *ownerBackedPeer) Close() error { return nil }

// v3 新增语义：不是 Owner 的节点必须拒绝 owner-local 请求，而不是擅自回源。
func TestServerGetRejectsRequestWhenNotOwner(t *testing.T) {
	cleanupGroups(t)
	peer := &fakePeer{value: []byte("wrong-peer")}
	picker := newFakePicker("remote", "self", peer)
	var loads atomic.Int64
	NewGroup("server-not-owner", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		loads.Add(1)
		return []byte("owner-source"), nil
	}), WithPeers(picker), WithExpiration(time.Minute))

	s := &Server{}
	// v4+ maps ownership rejection to a stable gRPC status; the client
	// classifies FailedPrecondition back into ErrTopologyChanged.
	if _, err := s.Get(context.Background(), &pb.Request{Group: "server-not-owner", Key: "k"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("non-owner server error=%v, want codes.FailedPrecondition", err)
	}
	if peer.calls.Load() != 0 {
		t.Fatalf("non-owner server rerouted the request %d times", peer.calls.Load())
	}
	if loads.Load() != 0 {
		t.Fatalf("non-owner server loaded from its own source %d times", loads.Load())
	}
}

func TestRequestsFromRemoteNodesConvergeOnOwnerSingleflight(t *testing.T) {
	cleanupGroups(t)
	var loads atomic.Int64
	owner := NewGroup("owner", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		loads.Add(1)
		time.Sleep(20 * time.Millisecond)
		return []byte("value"), nil
	}), WithExpiration(time.Minute))
	peer := &ownerBackedPeer{owner: owner}
	picker := newFakePicker("owner-node", "requester-node", peer)
	requester := NewGroup("requester", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		return nil, errors.New("requester source should not run")
	}), WithPeers(picker))

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := requester.Get(context.Background(), "same-key")
			if err != nil || v.String() != "value" {
				t.Errorf("get=%q err=%v", v.String(), err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if loads.Load() != 1 {
		t.Fatalf("owner source loads=%d, want 1", loads.Load())
	}
}

func TestOwnerTTLRefreshesFromSource(t *testing.T) {
	cleanupGroups(t)
	var current atomic.Value
	current.Store("v1")
	var loads atomic.Int64
	g := NewGroup("owner-ttl", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		loads.Add(1)
		return []byte(current.Load().(string)), nil
	}), WithExpiration(25*time.Millisecond))

	first, err := g.Get(context.Background(), "k")
	if err != nil || first.String() != "v1" {
		t.Fatalf("first=%q err=%v", first.String(), err)
	}
	current.Store("v2")
	withinTTL, err := g.Get(context.Background(), "k")
	if err != nil || withinTTL.String() != "v1" {
		t.Fatalf("within TTL=%q err=%v, want cached v1", withinTTL.String(), err)
	}
	time.Sleep(35 * time.Millisecond)
	afterTTL, err := g.Get(context.Background(), "k")
	if err != nil || afterTTL.String() != "v2" {
		t.Fatalf("after TTL=%q err=%v, want refreshed v2", afterTTL.String(), err)
	}
	if loads.Load() != 2 {
		t.Fatalf("source loads=%d, want 2", loads.Load())
	}
}

func TestExplicitCacheModeSupportsSetGetDeleteWithoutGetter(t *testing.T) {
	cleanupGroups(t)
	g := NewGroup("explicit", 1<<20, nil, WithExpiration(time.Minute))
	if _, err := g.Get(context.Background(), "k"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("initial miss err=%v, want ErrCacheMiss", err)
	}
	if err := g.Set(context.Background(), "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	v, err := g.Get(context.Background(), "k")
	if err != nil || v.String() != "v1" {
		t.Fatalf("get=%q err=%v", v.String(), err)
	}
	if err := g.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Get(context.Background(), "k"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("after delete err=%v, want ErrCacheMiss", err)
	}
}

func TestRemoteSetRefreshesWriterNearCache(t *testing.T) {
	cleanupGroups(t)
	owner := NewGroup("remote-set-owner", 1<<20, nil, WithExpiration(time.Minute))
	peer := &ownerBackedPeer{owner: owner}
	picker := newFakePicker("owner", "requester", peer)
	requester := NewGroup("remote-set-requester", 1<<20, nil,
		WithPeers(picker), WithNearCache(64<<10, time.Minute))

	if err := requester.Set(context.Background(), "k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	v, err := requester.Get(context.Background(), "k")
	if err != nil || v.String() != "v2" {
		t.Fatalf("get=%q err=%v", v.String(), err)
	}
	// Set performs one remote mutation; the following Get should hit near cache.
	if peer.calls.Load() != 1 {
		t.Fatalf("peer calls=%d, want 1", peer.calls.Load())
	}
	ownerValue, err := owner.getLocal(context.Background(), "k")
	if err != nil || ownerValue.String() != "v2" {
		t.Fatalf("owner value=%q err=%v", ownerValue.String(), err)
	}
}

func TestRemoteDeleteEvictsWriterNearCache(t *testing.T) {
	cleanupGroups(t)
	owner := NewGroup("remote-del-owner", 1<<20, nil, WithExpiration(time.Minute))
	if err := owner.setLocal("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	peer := &ownerBackedPeer{owner: owner}
	picker := newFakePicker("owner", "requester", peer)
	requester := NewGroup("remote-del-requester", 1<<20, nil,
		WithPeers(picker), WithNearCache(64<<10, time.Minute))

	if _, err := requester.Get(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if err := requester.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.getLocal(context.Background(), "k"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("owner after delete err=%v, want miss", err)
	}
	if _, ok := requester.nearCache.Get(context.Background(), requester.nearKey(picker.Epoch(), "k")); ok {
		t.Fatal("requester near-cache still contains deleted key")
	}
}

func TestInflightLoadDoesNotOverwriteConcurrentSet(t *testing.T) {
	cleanupGroups(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	g := NewGroup("inflight-set", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		once.Do(func() { close(started) })
		<-release
		return []byte("stale-from-source"), nil
	}))

	result := make(chan ByteView, 1)
	errCh := make(chan error, 1)
	go func() {
		v, err := g.Get(context.Background(), "k")
		result <- v
		errCh <- err
	}()
	<-started
	if err := g.Set(context.Background(), "k", []byte("fresh")); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatalf("inflight get returned err: %v", err)
	}
	if got := (<-result).String(); got != "fresh" {
		t.Fatalf("inflight get=%q, want fresh", got)
	}
	v, err := g.Get(context.Background(), "k")
	if err != nil || v.String() != "fresh" {
		t.Fatalf("final get=%q err=%v, want fresh", v.String(), err)
	}
}

func TestInflightLoadDoesNotResurrectDeletedEntry(t *testing.T) {
	cleanupGroups(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	g := NewGroup("inflight-delete", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		call := calls.Add(1)
		if call == 1 {
			close(started)
			<-release
			return []byte("stale-from-source"), nil
		}
		return nil, errors.New("source no longer has key")
	}))

	firstErr := make(chan error, 1)
	go func() {
		_, err := g.Get(context.Background(), "k")
		firstErr <- err
	}()
	<-started
	if err := g.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-firstErr; !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("superseded inflight get err=%v, want ErrCacheMiss", err)
	}
	if _, ok := g.mainCache.Get(context.Background(), "k"); ok {
		t.Fatal("deleted key was resurrected in owner cache")
	}
	if _, err := g.Get(context.Background(), "k"); err == nil {
		t.Fatal("second get unexpectedly succeeded after source deletion")
	}
	if calls.Load() != 2 {
		t.Fatalf("source calls=%d, want 2", calls.Load())
	}
}

func TestOwnerCacheIsInvalidatedAcrossMembershipEpochs(t *testing.T) {
	cleanupGroups(t)
	var source atomic.Value
	source.Store("v1")
	picker := newFakePicker("self", "self", nil)
	g := NewGroup("owner-epoch", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		return []byte(source.Load().(string)), nil
	}), WithPeers(picker))

	first, err := g.Get(context.Background(), "k")
	if err != nil || first.String() != "v1" {
		t.Fatalf("first=%q err=%v", first.String(), err)
	}
	picker.setOwner("other")
	source.Store("v2")
	picker.setOwner("self")

	second, err := g.Get(context.Background(), "k")
	if err != nil || second.String() != "v2" {
		t.Fatalf("after ownership return=%q err=%v, want v2", second.String(), err)
	}
}

type callbackPeer struct {
	mu      sync.Mutex
	values  [][]byte
	calls   int
	onFirst func()
}

func (p *callbackPeer) Get(context.Context, string, string) ([]byte, error) {
	p.mu.Lock()
	idx := p.calls
	p.calls++
	var value []byte
	if idx < len(p.values) {
		value = cloneBytes(p.values[idx])
	} else {
		value = cloneBytes(p.values[len(p.values)-1])
	}
	cb := p.onFirst
	if idx == 0 {
		p.onFirst = nil
	}
	p.mu.Unlock()
	if cb != nil {
		cb()
	}
	return value, nil
}
func (p *callbackPeer) Set(context.Context, string, string, []byte) error    { return nil }
func (p *callbackPeer) Delete(context.Context, string, string) (bool, error) { return true, nil }
func (p *callbackPeer) Close() error                                         { return nil }

func TestRemoteReadRetriesWhenMembershipEpochChanges(t *testing.T) {
	cleanupGroups(t)
	picker := newFakePicker("remote", "self", nil)
	peer := &callbackPeer{values: [][]byte{[]byte("old-topology"), []byte("new-topology")}}
	picker.peer = peer
	peer.onFirst = func() { picker.epoch.Add(1) }
	g := NewGroup("near-epoch-race", 1<<20, nil,
		WithPeers(picker), WithNearCache(64<<10, time.Minute))

	v, err := g.Get(context.Background(), "k")
	if err != nil || v.String() != "new-topology" {
		t.Fatalf("get=%q err=%v, want retried new-topology value", v.String(), err)
	}
	peer.mu.Lock()
	calls := peer.calls
	peer.mu.Unlock()
	if calls != 2 {
		t.Fatalf("peer calls=%d, want 2 after epoch change", calls)
	}
	if _, ok := g.nearCache.Get(context.Background(), g.nearKey(picker.Epoch(), "k")); !ok {
		t.Fatal("stable-epoch result was not cached")
	}
}

func TestRemoteOwnerRejectionIsRetried(t *testing.T) {
	cleanupGroups(t)
	peer := &fakePeer{err: fmt.Errorf("get from owner: %w", ErrNotOwner)}
	picker := newFakePicker("remote", "self", peer)
	g := NewGroup("remote-owner-retry", 1<<20, nil, WithPeers(picker))

	_, err := g.Get(context.Background(), "k")
	if calls := peer.calls.Load(); calls < 2 {
		t.Fatalf("remote owner rejection was not retried: calls=%d", calls)
	}
	if !errors.Is(err, ErrTopologyChanged) {
		t.Fatalf("final error=%v, want ErrTopologyChanged", err)
	}
}
func TestNilContextOnTopologyRetry(t *testing.T) {
	cleanupGroups(t)
	peer := &fakePeer{err: fmt.Errorf("get from owner: %w", ErrNotOwner)}
	picker := newFakePicker("remote", "self", peer)
	g := NewGroup("nil-context-retry", 1<<20, nil, WithPeers(picker))

	_, err := g.Get(nil, "k")
	if !errors.Is(err, ErrTopologyChanged) {
		t.Fatalf("Get(nil) error=%v, want ErrTopologyChanged", err)
	}
	if calls := peer.calls.Load(); calls < 2 {
		t.Fatalf("Get(nil) did not exercise retry path: calls=%d", calls)
	}
}
