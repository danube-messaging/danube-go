package danube

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/danube-messaging/danube-go/proto" // Path to your generated proto package
)

// LookupResult holds the result of a topic lookup
type LookupResult struct {
	ResponseType proto.TopicLookupResponse_LookupType
	Addr         string
}

// LookupService handles lookup operations
type lookupService struct {
	cnxManager  *connectionManager
	authService *AuthService
	requestID   atomic.Uint64
}

// NewLookupService creates a new instance of LookupService
func NewLookupService(cnxManager *connectionManager, authService *AuthService) *lookupService {
	return &lookupService{
		cnxManager:  cnxManager,
		authService: authService,
		requestID:   atomic.Uint64{},
	}
}

// LookupTopic performs the topic lookup request
func (ls *lookupService) lookupTopic(ctx context.Context, addr string, topic string) (*LookupResult, error) {
	conn, err := ls.cnxManager.getConnection(addr, addr)
	if err != nil {
		return nil, err
	}

	client := proto.NewDiscoveryClient(conn.grpcConn)

	lookupRequest := &proto.TopicLookupRequest{
		RequestId: ls.requestID.Add(1),
		Topic:     topic,
	}

	ctxWithAuth, err := ls.authService.attachTokenIfNeeded(ctx, ls.cnxManager.connectionOptions.APIKey, addr)
	if err != nil {
		return nil, err
	}

	response, err := client.TopicLookup(ctxWithAuth, lookupRequest)
	if err != nil {
		return nil, err
	}

	return &LookupResult{
		ResponseType: response.GetResponseType(),
		Addr:         response.GetBrokerServiceUrl(),
	}, nil
}

// LookupTopic performs the topic lookup request
func (ls *lookupService) topicPartitions(ctx context.Context, addr string, topic string) ([]string, error) {
	conn, err := ls.cnxManager.getConnection(addr, addr)
	if err != nil {
		return nil, err
	}

	client := proto.NewDiscoveryClient(conn.grpcConn)

	lookupRequest := &proto.TopicLookupRequest{
		RequestId: ls.requestID.Add(1),
		Topic:     topic,
	}

	ctxWithAuth, err := ls.authService.attachTokenIfNeeded(ctx, ls.cnxManager.connectionOptions.APIKey, addr)
	if err != nil {
		return nil, err
	}

	response, err := client.TopicPartitions(ctxWithAuth, lookupRequest)
	if err != nil {
		return nil, err
	}

	return response.GetPartitions(), nil

}

// HandleLookup processes the lookup request and returns the appropriate URI
func (ls *lookupService) handleLookup(ctx context.Context, addr string, topic string) (string, error) {
	lookupResult, err := ls.lookupTopic(ctx, addr, topic)
	if err != nil {
		return "", err
	}

	switch lookupResult.ResponseType {
	case proto.TopicLookupResponse_Redirect:
		return lookupResult.Addr, nil
	case proto.TopicLookupResponse_Connect:
		return addr, nil
	case proto.TopicLookupResponse_Failed:
		return "", errors.New("lookup failed")
	default:
		return "", errors.New("unknown lookup type")
	}
}
