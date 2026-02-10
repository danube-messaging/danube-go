package danube

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// RpcConnection wraps a gRPC client connection.
type rpcConnection struct {
	grpcConn *grpc.ClientConn
}

// DialOption is a function that configures gRPC dial options.
type DialOption func(*[]grpc.DialOption)

// WithKeepAliveInterval configures the keepalive interval for the connection.
func WithKeepAliveInterval(interval time.Duration) DialOption {
	return func(opts *[]grpc.DialOption) {
		*opts = append(*opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: interval,
		}))
	}
}

// WithConnectionTimeout configures the connection timeout for the connection.
func WithConnectionTimeout(timeout time.Duration) DialOption {
	return func(opts *[]grpc.DialOption) {
		*opts = append(*opts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			dialer := &net.Dialer{Timeout: timeout}
			return dialer.DialContext(ctx, "tcp", addr)
		}))
	}
}

// NewRpcConnection creates a new RpcConnection with the given options.
func newRpcConnection(connectURL string, options ConnectionOptions) (*rpcConnection, error) {
	var dialOptions []grpc.DialOption

	// Apply transport credentials
	if options.UseTLS {
		if options.TLSConfig == nil {
			return nil, fmt.Errorf("TLS is enabled but no TLS config provided")
		}
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(options.TLSConfig)))
	} else {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Apply additional options
	for _, opt := range options.DialOptions {
		opt(&dialOptions)
	}

	// the server send the address with http(s), required by Rust tonic client
	// therefore needs to be trimmed here
	urlTrimmed := strings.TrimPrefix(connectURL, "http://")
	urlTrimmed = strings.TrimPrefix(urlTrimmed, "https://")

	conn, err := grpc.NewClient(urlTrimmed, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	return &rpcConnection{grpcConn: conn}, nil
}
