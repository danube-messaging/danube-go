package danube

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const internalBrokerHeader = "x-danube-internal-broker"

// authService handles JWT token insertion into gRPC request metadata.
//
// With JWT-first authentication, the client uses a pre-generated JWT token
// (from `danube-admin security tokens create`) that is sent as
// `Authorization: Bearer <token>` on every gRPC request.
type authService struct {
	cnxManager *connectionManager
}

func newAuthService(cnxManager *connectionManager) *authService {
	return &authService{cnxManager: cnxManager}
}

// attachTokenIfNeeded returns a context with the authorization Bearer header
// if a token is configured (static or via supplier). It also attaches the
// internal broker header when set.
func (as *authService) attachTokenIfNeeded(ctx context.Context, addr string) (context.Context, error) {
	token := as.cnxManager.connectionOptions.resolveToken()
	if token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}

	if as.cnxManager.connectionOptions.InternalBroker != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, internalBrokerHeader, as.cnxManager.connectionOptions.InternalBroker)
	}

	return ctx, nil
}
