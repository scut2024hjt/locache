package locache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scut2024hjt/locache/pb"
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
func (p *fakePicker) PickOwner(string) (string, Peer, bool, bool) {
	if p.owner == "" {
		return "", nil, false, false
	}
	if p.owner == p.self {
		return p.owner, nil, true, true
	}
	return p.owner, p.peer, false, true
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
	picker := newFakePicker("remote", "self", peer)
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
