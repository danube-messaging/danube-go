package danube

import (
	"context"
	"sync"
	"time"

	"github.com/danube-messaging/danube-go/proto"
	"google.golang.org/grpc/metadata"
)

const tokenExpirySecs = 3600

// AuthService manages authentication tokens for the Danube client.
type AuthService struct {
	cnxManager *connectionManager
	mu         sync.Mutex
	token      string
	expiry     time.Time
}

func newAuthService(cnxManager *connectionManager) *AuthService {
	return &AuthService{cnxManager: cnxManager}
}

func (as *AuthService) authenticateClient(ctx context.Context, addr string, apiKey string) (string, error) {
	conn, err := as.cnxManager.getConnection(addr, addr)
	if err != nil {
		return "", err
	}

	client := proto.NewAuthServiceClient(conn.grpcConn)
	resp, err := client.Authenticate(ctx, &proto.AuthRequest{ApiKey: apiKey})
	if err != nil {
		return "", err
	}

	token := resp.GetToken()
	as.mu.Lock()
	as.token = token
	as.expiry = time.Now().Add(time.Duration(tokenExpirySecs) * time.Second)
	as.mu.Unlock()

	return token, nil
}

func (as *AuthService) getValidToken(ctx context.Context, addr string, apiKey string) (string, error) {
	as.mu.Lock()
	token := as.token
	expiry := as.expiry
	as.mu.Unlock()

	if token != "" && time.Now().Before(expiry) {
		return token, nil
	}

	return as.authenticateClient(ctx, addr, apiKey)
}

// attachTokenIfNeeded returns a context with the authorization header if apiKey is set.
func (as *AuthService) attachTokenIfNeeded(ctx context.Context, apiKey string, addr string) (context.Context, error) {
	if apiKey == "" {
		return ctx, nil
	}

	token, err := as.getValidToken(ctx, addr, apiKey)
	if err != nil {
		return ctx, err
	}

	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token), nil
}
