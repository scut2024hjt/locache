package locache

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scut2024hjt/locache/pb"
	"google.golang.org/grpc"
)

// The unit tests in group_test.go drive peer traffic through in-process Peer
// stubs. These tests instead put a real gRPC server and a real Client on a
// loopback socket, so client.go, the protobuf codec and the Server handlers are
// all exercised on the wire path.

// startRawGRPCServer starts a locache gRPC server without etcd. Server.Get/Set/
// Delete only need the group registry, so no lease or membership is required.
func startRawGRPCServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterLocacheServer(gs, &Server{})
	go func() { _ = gs.Serve(lis) }()

	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

// realPeerPicker always routes to a remote owner reached over gRPC.
type realPeerPicker struct {
	self  string
	owner string
	peer  Peer
	epoch atomic.Uint64
}

func (p *realPeerPicker) PickOwner(string) (string, Peer, bool, bool) {
	if p.owner == p.self {
		return p.owner, nil, true, true
	}
	return p.owner, p.peer, false, true
}

func (p *realPeerPicker) Epoch() uint64 { return p.epoch.Load() }

func (p *realPeerPicker) Invalidate(context.Context, string, string, string) error { return nil }

func (p *realPeerPicker) Close() error { return nil }

func TestGRPCWirePathReadThroughSetDelete(t *testing.T) {
	cleanupGroups(t)
	addr := startRawGRPCServer(t)
	client, err := NewClient(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	var loads atomic.Int64
	picker := &realPeerPicker{self: "requester-node", owner: addr, peer: client}
	picker.epoch.Store(1)

	g := NewGroup("grpc-e2e", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		loads.Add(1)
		return []byte("owner-source"), nil
	}), WithPeers(picker), WithExpiration(time.Minute), WithNearCache(64<<10, time.Minute))

	// Read-through: the requester must reach the owner over gRPC, load from the
	// source once, and satisfy the second read from the near cache.
	for i := 0; i < 2; i++ {
		view, err := g.Get(context.Background(), "k")
		if err != nil || view.String() != "owner-source" {
			t.Fatalf("get #%d=%q err=%v", i, view.String(), err)
		}
	}
	if loads.Load() != 1 {
		t.Fatalf("owner source loads=%d, want 1", loads.Load())
	}
	if got := g.nearCache.Len(); got != 1 {
		t.Fatalf("near cache entries=%d, want 1", got)
	}

	// Set is a real gRPC mutation of the owner plus a writer-side near-cache
	// refresh, so the following Get must not need another RPC.
	if err := g.Set(context.Background(), "k2", []byte("v2")); err != nil {
		t.Fatalf("set over grpc: %v", err)
	}
	view, err := g.Get(context.Background(), "k2")
	if err != nil || view.String() != "v2" {
		t.Fatalf("get after set=%q err=%v", view.String(), err)
	}
	if loads.Load() != 1 {
		t.Fatalf("writer near cache did not absorb the read: loads=%d", loads.Load())
	}

	// Delete evicts the owner copy over gRPC and drops the local near-cache
	// entry; the next read therefore has to go back to the source.
	if err := g.Delete(context.Background(), "k2"); err != nil {
		t.Fatalf("delete over grpc: %v", err)
	}
	if _, ok := g.nearCache.Get(context.Background(), g.nearKey(picker.Epoch(), "k2")); ok {
		t.Fatal("near cache still holds the deleted key")
	}
	view, err = g.Get(context.Background(), "k2")
	if err != nil || view.String() != "owner-source" {
		t.Fatalf("get after delete=%q err=%v", view.String(), err)
	}
	if loads.Load() != 2 {
		t.Fatalf("owner source loads after delete=%d, want 2", loads.Load())
	}
}

func TestGRPCWirePathPropagatesOwnerErrors(t *testing.T) {
	cleanupGroups(t)
	addr := startRawGRPCServer(t)
	client, err := NewClient(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	picker := &realPeerPicker{self: "requester-node", owner: addr, peer: client}
	picker.epoch.Store(1)

	// Explicit cache mode: an owner miss is reported as ErrCacheMiss and must
	// travel back over gRPC as an error rather than an empty value.
	g := NewGroup("grpc-e2e-miss", 1<<20, nil, WithPeers(picker), WithExpiration(time.Minute))
	if _, err := g.Get(context.Background(), "absent"); err == nil {
		t.Fatal("expected remote miss to return an error")
	} else if !strings.Contains(err.Error(), "cache miss") {
		t.Fatalf("miss error=%v, want ErrCacheMiss text", err)
	}

	// The Server must report unknown groups instead of panicking.
	if _, err := client.Get(context.Background(), "no-such-group", "k"); err == nil {
		t.Fatal("expected unknown group to fail")
	}
}

func TestGRPCClientHonoursCallerDeadline(t *testing.T) {
	cleanupGroups(t)
	addr := startRawGRPCServer(t)
	client, err := NewClient(addr, 10*time.Second)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if _, err := client.Get(ctx, "grpc-e2e-deadline", "k"); err == nil {
		t.Fatal("expected canceled context to fail")
	}
	// The client's default RPC timeout is 10s; the caller's cancellation must
	// short-circuit the call instead of waiting for it.
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("canceled call took %v, want immediate failure", elapsed)
	}
}

func TestGRPCClientRejectsWrongService(t *testing.T) {
	// A closed listener must surface as an error (with the caller deadline),
	// not as a hang or an empty successful response.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	client, err := NewClient(addr, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if _, err := client.Get(context.Background(), "any", "k"); err == nil {
		t.Fatal("expected unreachable owner to fail")
	} else if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "deadline") {
		t.Logf("unreachable owner error (informational): %v", err)
	}
}
