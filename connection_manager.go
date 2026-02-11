package danube

import (
	"crypto/tls"
	"sync"
)

type BrokerAddress struct {
	ConnectURL string
	BrokerURL  string
	Proxy      bool
}

type connectionStatus struct {
	Connected    *rpcConnection
	Disconnected bool
}

type ConnectionOptions struct {
	DialOptions []DialOption
	TLSConfig   *tls.Config
	UseTLS      bool
	APIKey      string
}

type connectionManager struct {
	connections       map[BrokerAddress]*connectionStatus
	connectionOptions ConnectionOptions
	connectionsMutex  sync.Mutex
}

// NewConnectionManager creates a new ConnectionManager.
func newConnectionManager(options ConnectionOptions) *connectionManager {
	return &connectionManager{
		connections:       make(map[BrokerAddress]*connectionStatus),
		connectionOptions: options,
	}
}

func (cm *connectionManager) getConnection(brokerURL, connectURL string) (*rpcConnection, error) {
	cm.connectionsMutex.Lock()
	defer cm.connectionsMutex.Unlock()

	proxy := brokerURL != connectURL
	broker := BrokerAddress{
		ConnectURL: connectURL,
		BrokerURL:  brokerURL,
		Proxy:      proxy,
	}

	status, exists := cm.connections[broker]
	if exists && status.Connected != nil {
		return status.Connected, nil
	}

	rpcConn, err := newRpcConnection(connectURL, cm.connectionOptions)
	if err != nil {
		return nil, err
	}

	if !exists {
		cm.connections[broker] = &connectionStatus{}
	}
	cm.connections[broker].Connected = rpcConn
	return rpcConn, nil
}
