package danube

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/danube-messaging/danube-go/proto"
)

type healthCheckService struct {
	cnxManager  *connectionManager
	authService *authService
	requestID   atomic.Uint64
}

func newHealthCheckService(cnxManager *connectionManager, authService *authService) *healthCheckService {
	return &healthCheckService{
		cnxManager:  cnxManager,
		authService: authService,
		requestID:   atomic.Uint64{},
	}
}

func (hcs *healthCheckService) StartHealthCheck(
	ctx context.Context,
	connectURL string,
	brokerAddr string,
	proxy bool,
	clientType int32,
	clientID uint64,
	stopSignal *atomic.Bool,
) error {
	conn, err := hcs.cnxManager.getConnection(brokerAddr, connectURL)
	if err != nil {
		return err
	}

	apiKey := hcs.cnxManager.connectionOptions.APIKey
	connectURLCopy := connectURL
	brokerAddrCopy := brokerAddr
	proxyCopy := proxy

	client := proto.NewHealthCheckClient(conn.grpcConn)
	go func() {
		for {
			err := healthCheck(ctx, client, hcs.requestID.Add(1), clientType, clientID, stopSignal, apiKey, connectURLCopy, brokerAddrCopy, proxyCopy, hcs.authService)
			if err != nil {
				return
			}
			time.Sleep(5 * time.Second)
		}
	}()
	return nil
}

func healthCheck(
	ctx context.Context,
	client proto.HealthCheckClient,
	requestID uint64,
	clientType int32,
	clientID uint64,
	stopSignal *atomic.Bool,
	apiKey string,
	connectURL string,
	brokerAddr string,
	proxy bool,
	authService *authService,
) error {
	healthRequest := &proto.HealthCheckRequest{
		RequestId: requestID,
		Client:    proto.HealthCheckRequest_ClientType(clientType),
		Id:        clientID,
	}

	ctxWithAuth, err := authService.attachTokenIfNeeded(ctx, apiKey, connectURL)
	if err != nil {
		return err
	}
	ctxWithProxy := insertProxyHeader(ctxWithAuth, brokerAddr, proxy)

	response, err := client.HealthCheck(ctxWithProxy, healthRequest)
	if err != nil {
		return err
	}

	if response.GetStatus() == proto.HealthCheckResponse_CLOSE {
		stopSignal.Store(true)
		return nil
	}
	return nil
}
