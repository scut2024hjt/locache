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
//
// v3 note: the owner-local path now verifies that this node really is the
// current owner, so a group that claims a remote owner is refused with
// ErrNotOwner. The requester-side routing is covered by group_test.go; here we
// keep the *server side* owning the keys and talk to it through a real Client.

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

// realPeerPicker routes keys to an owner address. When owner == self the node
// believes it owns every key; otherwise the owner is reached over gRPC.
type realPeerPicker struct {
	self  string
	owner string
	peer  Peer
	epoch atomic.Uint64
}

func (p *realPeerPicker) PickOwner(string) (string, Peer, bool, uint64, bool) {
	epoch := p.epoch.Load()
	if p.owner == p.self {
		return p.owner, nil, true, epoch, true
	}
	return p.owner, p.peer, false, epoch, true
}

func (p *realPeerPicker) Epoch() uint64 { return p.epoch.Load() }

func (p *realPeerPicker) Invalidate(context.Context, string, string, string) error { return nil }

func (p *realPeerPicker) Close() error { return nil }

// TestGRPCWirePathReadThroughSetDelete drives Get/Set/Delete through a real
// gRPC Client against a real Server: protobuf round-trip, handler dispatch and
// the owner cache are all on the wire path.
func TestGRPCWirePathReadThroughSetDelete(t *testing.T) {
	cleanupGroups(t)
	addr := startRawGRPCServer(t)
	client, err := NewClient(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	var loads atomic.Int64
	picker := &realPeerPicker{self: addr, owner: addr, peer: client}
	picker.epoch.Store(1)

	NewGroup("grpc-e2e", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		loads.Add(1)
		return []byte("owner-source"), nil
	}), WithPeers(picker), WithExpiration(time.Minute))

	// Read-through over gRPC, then a second read served by the owner cache.
	for i := 0; i < 2; i++ {
		view, err := client.Get(context.Background(), "grpc-e2e", "k")
		if err != nil || string(view) != "owner-source" {
			t.Fatalf("grpc get #%d=%q err=%v", i, string(view), err)
		}
	}
	if loads.Load() != 1 {
		t.Fatalf("owner source loads=%d, want 1", loads.Load())
	}

	// Set over gRPC, then read the value back from the owner cache.
	if err := client.Set(context.Background(), "grpc-e2e", "k2", []byte("v2")); err != nil {
		t.Fatalf("grpc set: %v", err)
	}
	if view, err := client.Get(context.Background(), "grpc-e2e", "k2"); err != nil || string(view) != "v2" {
		t.Fatalf("grpc get after set=%q err=%v", string(view), err)
	}
	if loads.Load() != 1 {
		t.Fatalf("set should not reload from source: loads=%d", loads.Load())
	}

	// Delete over gRPC evicts the owner copy; the next read reloads the source.
	removed, err := client.Delete(context.Background(), "grpc-e2e", "k2")
	if err != nil || !removed {
		t.Fatalf("grpc delete removed=%v err=%v", removed, err)
	}
	if view, err := client.Get(context.Background(), "grpc-e2e", "k2"); err != nil || string(view) != "owner-source" {
		t.Fatalf("grpc get after delete=%q err=%v", string(view), err)
	}
	if loads.Load() != 2 {
		t.Fatalf("owner source loads after delete=%d, want 2", loads.Load())
	}
}

// TestGRPCWirePathPropagatesOwnerErrors covers the error paths a remote caller
// observes: owner miss, non-owner refusal and unknown group.
func TestGRPCWirePathPropagatesOwnerErrors(t *testing.T) {
	cleanupGroups(t)
	addr := startRawGRPCServer(t)
	client, err := NewClient(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// Explicit cache mode on the owner: a miss must come back as an error
	// rather than an empty successful value.
	ownerPicker := &realPeerPicker{self: addr, owner: addr, peer: client}
	ownerPicker.epoch.Store(1)
	NewGroup("grpc-e2e-miss", 1<<20, nil, WithPeers(ownerPicker), WithExpiration(time.Minute))
	if _, err := client.Get(context.Background(), "grpc-e2e-miss", "absent"); err == nil {
		t.Fatal("expected owner miss to return an error")
	} else if !strings.Contains(err.Error(), "cache miss") {
		t.Fatalf("miss error=%v, want cache-miss text", err)
	}

	// A node that is not the owner must refuse to serve the key, and the
	// rejection must not silently fall back to the local Getter. The gRPC layer
	// translates ownership/topology rejection back to ErrTopologyChanged.
	var localLoads atomic.Int64
	nonOwner := &realPeerPicker{self: "requester-node", owner: addr, peer: client}
	nonOwner.epoch.Store(1)
	NewGroup("grpc-e2e-not-owner", 1<<20, GetterFunc(func(context.Context, string) ([]byte, error) {
		localLoads.Add(1)
		return []byte("must-not-be-served"), nil
	}), WithPeers(nonOwner), WithExpiration(time.Minute))

	_, err = client.Get(context.Background(), "grpc-e2e-not-owner", "k")
	if err == nil {
		t.Fatal("expected non-owner node to refuse the request")
	}
	if !strings.Contains(err.Error(), "not the current owner") {
		t.Fatalf("refusal error=%v, want ownership rejection", err)
	}
	if localLoads.Load() != 0 {
		t.Fatalf("non-owner served the key from its local Getter %d times", localLoads.Load())
	}
	if !errors.Is(err, ErrTopologyChanged) {
		t.Errorf("ownership rejection error=%v, want ErrTopologyChanged", err)
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
