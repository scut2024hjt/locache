package locache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scut2024hjt/locache/consistenthash"
	"github.com/scut2024hjt/locache/registry"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultSvcName = "locache"

type PeerPicker interface {
	// PickOwner returns the owner address. peer is nil when the owner is self.
	PickOwner(key string) (owner string, peer Peer, self bool, ok bool)
	// Epoch changes whenever this picker observes a membership change. Groups
	// use it to isolate requester-side near-cache entries across remapping.
	Epoch() uint64
	// Invalidate best-effort evicts local copies from remote peers except the
	// current owner. Owner mutation success is not rolled back if a peer cannot
	// be reached; near-cache TTL remains the stale-data fallback bound.
	Invalidate(ctx context.Context, group, key, exceptOwner string) error
	Close() error
}

type Peer interface {
	Get(ctx context.Context, group, key string) ([]byte, error)
	Set(ctx context.Context, group, key string, value []byte) error
	Delete(ctx context.Context, group, key string) (bool, error)
	Close() error
}

type ClientPicker struct {
	selfAddr     string
	svcName      string
	mu           sync.RWMutex
	ring         *consistenthash.Map
	members      map[string]struct{}
	clients      map[string]*Client
	etcdCli      *clientv3.Client
	endpoints    []string
	dialTO       time.Duration
	rpcTO        time.Duration
	hashReplicas int
	ctx          context.Context
	cancel       context.CancelFunc
	watchDone    chan struct{}
	closeOnce    sync.Once
	epoch        atomic.Uint64
}

type PickerOption func(*ClientPicker)

func WithServiceName(name string) PickerOption {
	return func(p *ClientPicker) {
		if name != "" {
			p.svcName = name
		}
	}
}

func WithPickerEtcdEndpoints(endpoints []string) PickerOption {
	return func(p *ClientPicker) {
		if len(endpoints) > 0 {
			p.endpoints = append([]string(nil), endpoints...)
		}
	}
}

func WithPickerDialTimeout(timeout time.Duration) PickerOption {
	return func(p *ClientPicker) {
		if timeout > 0 {
			p.dialTO = timeout
		}
	}
}

func WithPeerRPCTimeout(timeout time.Duration) PickerOption {
	return func(p *ClientPicker) {
		if timeout > 0 {
			p.rpcTO = timeout
		}
	}
}

func WithHashReplicas(replicas int) PickerOption {
	return func(p *ClientPicker) {
		if replicas > 0 {
			p.hashReplicas = replicas
			p.ring = consistenthash.New(consistenthash.WithReplicas(replicas))
		}
	}
}

func NewClientPicker(addr string, opts ...PickerOption) (*ClientPicker, error) {
	selfAddr, err := registry.NormalizeAddress(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &ClientPicker{
		selfAddr:     selfAddr,
		svcName:      defaultSvcName,
		ring:         consistenthash.New(),
		members:      make(map[string]struct{}),
		clients:      make(map[string]*Client),
		endpoints:    append([]string(nil), registry.DefaultConfig.Endpoints...),
		dialTO:       registry.DefaultConfig.DialTimeout,
		rpcTO:        3 * time.Second,
		hashReplicas: consistenthash.DefaultConfig.Replicas,
		ctx:          ctx,
		cancel:       cancel,
		watchDone:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: p.endpoints, DialTimeout: p.dialTO})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create etcd client: %w", err)
	}
	p.etcdCli = cli

	// Self is part of membership immediately, even before its etcd lease becomes
	// visible. This keeps the local ring semantically complete.
	p.members[p.selfAddr] = struct{}{}
	_ = p.ring.Add(p.selfAddr)
	p.epoch.Store(1)

	nextRev, err := p.refreshSnapshot()
	if err != nil {
		cancel()
		_ = cli.Close()
		return nil, err
	}
	go p.watchServiceChanges(nextRev)
	return p, nil
}

func (p *ClientPicker) refreshSnapshot() (int64, error) {
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()
	resp, err := p.etcdCli.Get(ctx, registry.ServicePrefix(p.svcName), clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("fetch service snapshot: %w", err)
	}

	desired := map[string]struct{}{p.selfAddr: {}}
	for _, kv := range resp.Kvs {
		addr := string(kv.Value)
		if addr == "" {
			addr = parseAddrFromKey(string(kv.Key), p.svcName)
		}
		if addr != "" {
			desired[addr] = struct{}{}
		}
	}
	p.replaceMembership(desired)
	return resp.Header.Revision + 1, nil
}

func (p *ClientPicker) replaceMembership(desired map[string]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if sameMembers(p.members, desired) {
		return
	}

	newRing := consistenthash.New(consistenthash.WithReplicas(p.hashReplicas))
	members := make([]string, 0, len(desired))
	for addr := range desired {
		members = append(members, addr)
	}
	sort.Strings(members)
	_ = newRing.Add(members...)

	newClients := make(map[string]*Client, len(desired)-1)
	for _, addr := range members {
		if addr == p.selfAddr {
			continue
		}
		if existing := p.clients[addr]; existing != nil {
			newClients[addr] = existing
			continue
		}
		client, err := NewClient(addr, p.rpcTO)
		if err != nil {
			logrus.Errorf("create peer client %s: %v", addr, err)
			continue
		}
		newClients[addr] = client
	}
	for addr, client := range p.clients {
		if _, keep := newClients[addr]; !keep {
			_ = client.Close()
		}
	}
	p.members = desired
	p.clients = newClients
	p.ring = newRing
	p.epoch.Add(1)
}

func sameMembers(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func (p *ClientPicker) watchServiceChanges(startRev int64) {
	defer close(p.watchDone)
	rev := startRev
	for p.ctx.Err() == nil {
		watchCh := p.etcdCli.Watch(
			p.ctx,
			registry.ServicePrefix(p.svcName),
			clientv3.WithPrefix(),
			clientv3.WithPrevKV(),
			clientv3.WithRev(rev),
		)
		watchFailed := false
		for resp := range watchCh {
			if err := resp.Err(); err != nil {
				logrus.Warnf("etcd watch failed: %v", err)
				watchFailed = true
				break
			}
			for _, event := range resp.Events {
				addr := ""
				switch event.Type {
				case clientv3.EventTypePut:
					addr = string(event.Kv.Value)
					if addr == "" {
						addr = parseAddrFromKey(string(event.Kv.Key), p.svcName)
					}
					p.addMember(addr)
				case clientv3.EventTypeDelete:
					addr = parseAddrFromKey(string(event.Kv.Key), p.svcName)
					if addr == "" && event.PrevKv != nil {
						addr = string(event.PrevKv.Value)
					}
					p.removeMember(addr)
				}
			}
			if resp.Header.Revision >= rev {
				rev = resp.Header.Revision + 1
			}
		}
		if p.ctx.Err() != nil {
			return
		}
		// A compacted revision or a broken watch is recovered by taking a fresh
		// snapshot and resuming from snapshotRevision+1.
		if watchFailed {
			time.Sleep(200 * time.Millisecond)
		}
		next, err := p.refreshSnapshot()
		if err != nil {
			logrus.Warnf("refresh membership after watch interruption: %v", err)
			time.Sleep(time.Second)
			continue
		}
		rev = next
	}
}

func (p *ClientPicker) addMember(addr string) {
	if addr == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.members[addr]; exists {
		return
	}
	p.members[addr] = struct{}{}
	_ = p.ring.Add(addr)
	if addr != p.selfAddr {
		client, err := NewClient(addr, p.rpcTO)
		if err != nil {
			logrus.Errorf("create peer client %s: %v", addr, err)
		} else {
			p.clients[addr] = client
		}
	}
	p.epoch.Add(1)
	logrus.Infof("locache member added: %s", addr)
}

func (p *ClientPicker) removeMember(addr string) {
	if addr == "" || addr == p.selfAddr {
		return
	}
	p.mu.Lock()
	if _, exists := p.members[addr]; !exists {
		p.mu.Unlock()
		return
	}
	delete(p.members, addr)
	_ = p.ring.Remove(addr)
	client := p.clients[addr]
	delete(p.clients, addr)
	p.epoch.Add(1)
	p.mu.Unlock()
	if client != nil {
		_ = client.Close()
	}
	logrus.Infof("locache member removed: %s", addr)
}

func (p *ClientPicker) PickOwner(key string) (string, Peer, bool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	owner := p.ring.Get(key)
	if owner == "" {
		return "", nil, false, false
	}
	if owner == p.selfAddr {
		return owner, nil, true, true
	}
	return owner, p.clients[owner], false, true
}

func (p *ClientPicker) Invalidate(ctx context.Context, group, key, exceptOwner string) error {
	p.mu.RLock()
	targets := make([]Peer, 0, len(p.clients))
	targetAddrs := make([]string, 0, len(p.clients))
	for addr, client := range p.clients {
		if addr == exceptOwner {
			continue
		}
		targets = append(targets, client)
		targetAddrs = append(targetAddrs, addr)
	}
	p.mu.RUnlock()

	if len(targets) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(targets))
	for i, peer := range targets {
		addr := targetAddrs[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := peer.Delete(ctx, group, key); err != nil {
				errCh <- fmt.Errorf("invalidate %s: %w", addr, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (p *ClientPicker) Epoch() uint64 { return p.epoch.Load() }

func (p *ClientPicker) Members() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	members := make([]string, 0, len(p.members))
	for addr := range p.members {
		members = append(members, addr)
	}
	sort.Strings(members)
	return members
}

func (p *ClientPicker) PrintPeers() {
	for _, addr := range p.Members() {
		log.Printf("- %s", addr)
	}
}

func (p *ClientPicker) Close() error {
	var closeErr error
	p.closeOnce.Do(func() {
		p.cancel()
		select {
		case <-p.watchDone:
		case <-time.After(2 * time.Second):
			closeErr = fmt.Errorf("timed out waiting for membership watch to stop")
		}

		p.mu.Lock()
		clients := p.clients
		p.clients = make(map[string]*Client)
		p.mu.Unlock()
		for _, client := range clients {
			if err := client.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
		if p.etcdCli != nil {
			if err := p.etcdCli.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

func parseAddrFromKey(key, svcName string) string {
	prefix := registry.ServicePrefix(svcName)
	if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
		return ""
	}
	return key[len(prefix):]
}
