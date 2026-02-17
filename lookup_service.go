package danube

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/danube-messaging/danube-go/proto" // Path to your generated proto package
)

type lookupResult struct {
	ResponseType proto.TopicLookupResponse_LookupType
	ConnectURL   string
	BrokerURL    string
	Proxy        bool
}

type lookupService struct {
	cnxManager  *connectionManager
	authService *authService
	requestID   atomic.Uint64
}

func newLookupService(cnxManager *connectionManager, authService *authService) *lookupService {
	return &lookupService{
		cnxManager:  cnxManager,
		authService: authService,
		requestID:   atomic.Uint64{},
	}
}

// LookupTopic performs the topic lookup request
func (ls *lookupService) lookupTopic(ctx context.Context, addr string, topic string) (*lookupResult, error) {
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

	return &lookupResult{
		ResponseType: response.GetResponseType(),
		ConnectURL:   response.GetConnectUrl(),
		BrokerURL:    response.GetBrokerUrl(),
		Proxy:        response.GetProxy(),
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

// handleLookup processes the lookup request and returns the broker address with connect/broker URLs and proxy flag.
func (ls *lookupService) handleLookup(ctx context.Context, addr string, topic string) (*brokerAddress, error) {
	lookupResult, err := ls.lookupTopic(ctx, addr, topic)
	if err != nil {
		return nil, err
	}

	switch lookupResult.ResponseType {
	case proto.TopicLookupResponse_Redirect, proto.TopicLookupResponse_Connect:
		return &brokerAddress{
			ConnectURL: lookupResult.ConnectURL,
			BrokerURL:  lookupResult.BrokerURL,
			Proxy:      lookupResult.Proxy,
		}, nil
	case proto.TopicLookupResponse_Failed:
		return nil, errors.New("topic lookup failed: topic may not exist or cluster is unavailable")
	default:
		return nil, errors.New("unknown lookup type")
	}
}
