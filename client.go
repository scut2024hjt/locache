package locache

import (
	"context"
	"fmt"
	"time"

	pb "github.com/scut2024hjt/locache/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Client struct {
	addr       string
	rpcTimeout time.Duration
	conn       *grpc.ClientConn
	grpcCli    pb.LocacheClient
}

var _ Peer = (*Client)(nil)

const peerInvalidationMetadataKey = "x-locache-invalidate"

func NewClient(addr string, rpcTimeout time.Duration) (*Client, error) {
	if rpcTimeout <= 0 {
		rpcTimeout = 3 * time.Second
	}
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, fmt.Errorf("create grpc client for %s: %w", addr, err)
	}
	return &Client{
		addr:       addr,
		rpcTimeout: rpcTimeout,
		conn:       conn,
		grpcCli:    pb.NewLocacheClient(conn),
	}, nil
}

func classifyRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %v", ErrTopologyChanged, err)
	case codes.NotFound:
		return fmt.Errorf("%w: %v", ErrCacheMiss, err)
	default:
		return err
	}
}

func (c *Client) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.rpcTimeout)
}

func (c *Client) Get(ctx context.Context, group, key string) ([]byte, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	resp, err := c.grpcCli.Get(callCtx, &pb.Request{Group: group, Key: key})
	if err != nil {
		return nil, fmt.Errorf("grpc get from %s: %w", c.addr, classifyRPCError(err))
	}
	return resp.GetValue(), nil
}

func (c *Client) Set(ctx context.Context, group, key string, value []byte) error {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	_, err := c.grpcCli.Set(callCtx, &pb.Request{Group: group, Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("grpc set on %s: %w", c.addr, classifyRPCError(err))
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, group, key string) (bool, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	resp, err := c.grpcCli.Delete(callCtx, &pb.Request{Group: group, Key: key})
	if err != nil {
		return false, fmt.Errorf("grpc delete on %s: %w", c.addr, classifyRPCError(err))
	}
	return resp.GetValue(), nil
}

func (c *Client) Invalidate(ctx context.Context, group, key string) error {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	callCtx = metadata.AppendToOutgoingContext(callCtx, peerInvalidationMetadataKey, "1")
	_, err := c.grpcCli.Delete(callCtx, &pb.Request{Group: group, Key: key})
	if err != nil {
		return fmt.Errorf("grpc invalidate on %s: %w", c.addr, classifyRPCError(err))
	}
	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
