package locache

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"crypto/tls"

	"github.com/sirupsen/logrus"
	pb "github.com/scut2024hjt/locache/pb"
	"github.com/scut2024hjt/locache/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Server 定义缓存服务器
type Server struct {
	pb.UnimplementedLocacheServer
	addr       string           // 服务地址
	svcName    string           // 服务名称
	groups     *sync.Map        // 缓存组
	grpcServer *grpc.Server     // gRPC服务器
	etcdCli    *clientv3.Client // etcd客户端
	stopCh     chan error       // 停止信号
	opts       *ServerOptions   // 服务器选项
}

// ServerOptions 服务器配置选项
type ServerOptions struct {
	EtcdEndpoints []string      // etcd端点
	DialTimeout   time.Duration // 连接超时
	MaxMsgSize    int           // 最大消息大小
	TLS           bool          // 是否启用TLS
	CertFile      string        // 证书文件
	KeyFile       string        // 密钥文件
}

// DefaultServerOptions 默认配置
var DefaultServerOptions = &ServerOptions{
	EtcdEndpoints: []string{"localhost:2379"},
	DialTimeout:   5 * time.Second,
	MaxMsgSize:    4 << 20, // 4MB
}

// ServerOption 定义选项函数类型
type ServerOption func(*ServerOptions)

// WithEtcdEndpoints 设置etcd端点
func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdEndpoints = endpoints
	}
}

// WithDialTimeout 设置连接超时
func WithDialTimeout(timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.DialTimeout = timeout
	}
}

// WithTLS 设置TLS配置
func WithTLS(certFile, keyFile string) ServerOption {
	return func(o *ServerOptions) {
		o.TLS = true
		o.CertFile = certFile
		o.KeyFile = keyFile
	}
}

// NewServer 创建新的服务器实例
func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	options := DefaultServerOptions
	for _, opt := range opts {
		opt(options)
	}

	// 创建etcd客户端
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   options.EtcdEndpoints,
		DialTimeout: options.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	// 创建gRPC服务器
	var serverOpts []grpc.ServerOption
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(options.MaxMsgSize))

	if options.TLS {
		creds, err := loadTLSCredentials(options.CertFile, options.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	srv := &Server{
		addr:       addr,
		svcName:    svcName,
		groups:     &sync.Map{},
		grpcServer: grpc.NewServer(serverOpts...),
		etcdCli:    etcdCli,
		stopCh:     make(chan error),
		opts:       options,
	}

	// 注册服务
	pb.RegisterLocacheServer(srv.grpcServer, srv)

	// 注册健康检查服务
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv.grpcServer, healthServer)
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)

	return srv, nil
}

// Start 启动服务器
func (s *Server) Start() error {
	// 启动gRPC服务器
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	// 注册到etcd
	stopCh := make(chan error)
	go func() {
		if err := registry.Register(s.svcName, s.addr, stopCh); err != nil {
			logrus.Errorf("failed to register service: %v", err)
			close(stopCh)
			return
		}
	}()

	logrus.Infof("Server starting at %s", s.addr)
	return s.grpcServer.Serve(lis)
}

// Stop 停止服务器
func (s *Server) Stop() {
	close(s.stopCh)
	s.grpcServer.GracefulStop()
	if s.etcdCli != nil {
		s.etcdCli.Close()
	}
}

// Get 实现Cache服务的Get方法
func (s *Server) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	view, err := group.Get(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: view.ByteSLice()}, nil
}

// Set 实现Cache服务的Set方法
func (s *Server) Set(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	// 从 context 中获取标记，如果没有则创建新的 context
	fromPeer := ctx.Value("from_peer")
	if fromPeer == nil {
		ctx = context.WithValue(ctx, "from_peer", true)
	}

	if err := group.Set(ctx, req.Key, req.Value); err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: req.Value}, nil
}

// Delete 实现Cache服务的Delete方法
func (s *Server) Delete(ctx context.Context, req *pb.Request) (*pb.ResponseForDelete, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	err := group.Delete(ctx, req.Key)
	return &pb.ResponseForDelete{Value: err == nil}, err
}

// loadTLSCredentials 加载TLS证书
func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	}), nil
}
