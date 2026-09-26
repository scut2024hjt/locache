package registry

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Config struct {
	Endpoints   []string
	DialTimeout time.Duration
	LeaseTTL    int64
}

var DefaultConfig = Config{
	Endpoints:   []string{"localhost:2379"},
	DialTimeout: 5 * time.Second,
	LeaseTTL:    10,
}

func ServicePrefix(svcName string) string {
	return fmt.Sprintf("/services/%s/", svcName)
}

func ServiceKey(svcName, addr string) string {
	return ServicePrefix(svcName) + addr
}

// Registration owns one etcd lease. The etcd client itself is owned by the
// caller and is intentionally not closed here.
type Registration struct {
	cli     *clientv3.Client
	leaseID clientv3.LeaseID
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
}

func Register(parent context.Context, cli *clientv3.Client, svcName, addr string, ttl int64) (*Registration, error) {
	if cli == nil {
		return nil, fmt.Errorf("nil etcd client")
	}
	if ttl <= 0 {
		ttl = DefaultConfig.LeaseTTL
	}

	ctx, cancel := context.WithCancel(parent)
	grantCtx, grantCancel := context.WithTimeout(ctx, 3*time.Second)
	lease, err := cli.Grant(grantCtx, ttl)
	grantCancel()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("grant etcd lease: %w", err)
	}

	putCtx, putCancel := context.WithTimeout(ctx, 3*time.Second)
	_, err = cli.Put(putCtx, ServiceKey(svcName, addr), addr, clientv3.WithLease(lease.ID))
	putCancel()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("register service in etcd: %w", err)
	}

	keepAliveCh, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("keep etcd lease alive: %w", err)
	}

	r := &Registration{cli: cli, leaseID: lease.ID, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-keepAliveCh:
				if !ok {
					if ctx.Err() == nil {
						logrus.Warnf("etcd keepalive channel closed for %s", addr)
					}
					return
				}
				if resp != nil {
					logrus.Debugf("renewed locache lease %d", resp.ID)
				}
			}
		}
	}()

	logrus.Infof("registered locache node %s as %s", addr, svcName)
	return r, nil
}

func (r *Registration) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var revokeErr error
	r.once.Do(func() {
		r.cancel()
		select {
		case <-r.done:
		case <-ctx.Done():
			revokeErr = ctx.Err()
		}
		revokeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := r.cli.Revoke(revokeCtx, r.leaseID); err != nil && revokeErr == nil {
			revokeErr = err
		}
	})
	return revokeErr
}

// NormalizeAddress turns wildcard listen addresses such as :8001 or
// 0.0.0.0:8001 into a routable advertised address. Explicit hostnames/IPs are
// preserved.
func NormalizeAddress(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("invalid address %q: %w", addr, err)
	}
	if port == "" {
		return "", fmt.Errorf("missing port in address %q", addr)
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host, err = GetLocalIP()
		if err != nil {
			return "", err
		}
	}
	host = strings.Trim(host, "[]")
	return net.JoinHostPort(host, port), nil
}

func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ip := ipNet.IP.To4(); ip != nil {
				return ip.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no routable IPv4 address found")
}
