package locache

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/scut2024hjt/locache/pb"
	"github.com/scut2024hjt/locache/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedLocacheServer
	listenAddr    string
	advertiseAddr string
	svcName       string
	grpcServer    *grpc.Server
	etcdCli       *clientv3.Client
	opts          ServerOptions
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex
	registration  *registry.Registration
	listener      net.Listener
	stopOnce      sync.Once
}

type ServerOptions struct {
	EtcdEndpoints []string
	DialTimeout   time.Duration
	LeaseTTL      int64
	MaxMsgSize    int
	TLS           bool
	CertFile      string
	KeyFile       string
	AdvertiseAddr string
}

var DefaultServerOptions = ServerOptions{
	EtcdEndpoints: []string{"localhost:2379"},
	DialTimeout:   5 * time.Second,
	LeaseTTL:      10,
	MaxMsgSize:    4 << 20,
}

type ServerOption func(*ServerOptions)

func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) { o.EtcdEndpoints = append([]string(nil), endpoints...) }
}
func WithDialTimeout(timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		if timeout > 0 {
			o.DialTimeout = timeout
		}
	}
}
func WithLeaseTTL(seconds int64) ServerOption {
	return func(o *ServerOptions) {
		if seconds > 0 {
			o.LeaseTTL = seconds
		}
	}
}
func WithAdvertiseAddress(addr string) ServerOption {
	return func(o *ServerOptions) { o.AdvertiseAddr = addr }
}

func WithTLS(certFile, keyFile string) ServerOption {
	return func(o *ServerOptions) {
		o.TLS = true
		o.CertFile = certFile
		o.KeyFile = keyFile
	}
}

func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	options := DefaultServerOptions
	options.EtcdEndpoints = append([]string(nil), DefaultServerOptions.EtcdEndpoints...)
	for _, opt := range opts {
		opt(&options)
	}
	if svcName == "" {
		svcName = defaultSvcName
	}
	advertiseSource := addr
	if options.AdvertiseAddr != "" {
		advertiseSource = options.AdvertiseAddr
	}
	advertiseAddr, err := registry.NormalizeAddress(advertiseSource)
	if err != nil {
		return nil, err
	}

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: options.EtcdEndpoints, DialTimeout: options.DialTimeout})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(options.MaxMsgSize),
		grpc.MaxSendMsgSize(options.MaxMsgSize),
	}
	if options.TLS {
		creds, err := loadTLSCredentials(options.CertFile, options.KeyFile)
		if err != nil {
			_ = etcdCli.Close()
			return nil, fmt.Errorf("load TLS credentials: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		listenAddr:    addr,
		advertiseAddr: advertiseAddr,
		svcName:       svcName,
		grpcServer:    grpc.NewServer(serverOpts...),
		etcdCli:       etcdCli,
		opts:          options,
		ctx:           ctx,
		cancel:        cancel,
	}
	pb.RegisterLocacheServer(s.grpcServer, s)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(s.grpcServer, healthServer)
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)
	return s, nil
}

// Address is the canonical address registered in etcd. Use the same value when
// constructing the local ClientPicker so every node names itself consistently.
func (s *Server) Address() string { return s.advertiseAddr }

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.listenAddr, err)
	}

	reg, err := registry.Register(s.ctx, s.etcdCli, s.svcName, s.advertiseAddr, s.opts.LeaseTTL)
	if err != nil {
		_ = lis.Close()
		return fmt.Errorf("register locache service: %w", err)
	}

	s.mu.Lock()
	s.registration = reg
	s.listener = lis
	s.mu.Unlock()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = reg.Close(ctx)
		cancel()
	}()

	err = s.grpcServer.Serve(lis)
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) && s.ctx.Err() == nil {
		return err
	}
	return nil
}

func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		s.cancel()
		s.mu.Lock()
		reg := s.registration
		s.mu.Unlock()
		if reg != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = reg.Close(ctx)
			cancel()
		}
		s.grpcServer.GracefulStop()
		if s.etcdCli != nil {
			_ = s.etcdCli.Close()
		}
	})
}

func mapGroupError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrNotOwner), errors.Is(err, ErrTopologyChanged), errors.Is(err, ErrNoOwner):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrCacheMiss):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrGroupClosed):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return err
	}
}

// Get is deliberately local-only. The caller already selected this node as the
// owner, so peer requests cannot recursively re-enter distributed routing.
func (s *Server) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}
	view, err := group.getLocal(ctx, req.Key)
	if err != nil {
		return nil, mapGroupError(err)
	}
	return &pb.ResponseForGet{Value: view.ByteSlice()}, nil
}

// Set is local-only. Distributed routing happened on the caller before this RPC.
func (s *Server) Set(_ context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}
	if err := group.setLocal(req.Key, req.Value); err != nil {
		return nil, mapGroupError(err)
	}
	return &pb.ResponseForGet{Value: req.Value}, nil
}

// Delete is an idempotent local eviction. Public Group.Delete uses it for the
// owner mutation, and mutation invalidation reuses it on non-owner peers to
// drop near-cache or stale previous-owner copies without recursive routing.
func (s *Server) Delete(ctx context.Context, req *pb.Request) (*pb.ResponseForDelete, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	// Replica invalidation is deliberately local-only and may target a
	// non-owner. A normal distributed Delete, however, must still be rejected
	// by an old owner so the requester can re-resolve the current owner.
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get(peerInvalidationMetadataKey)) > 0 {
		return &pb.ResponseForDelete{Value: group.deleteLocal(req.Key)}, nil
	}

	removed, err := group.deleteOwnerLocal(req.Key)
	if err != nil {
		return nil, mapGroupError(err)
	}
	return &pb.ResponseForDelete{Value: removed}, nil
}

func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}), nil
}
