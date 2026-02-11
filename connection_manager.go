package danube

import (
	"crypto/tls"
	"sync"
)

type brokerAddress struct {
	ConnectURL string
	BrokerURL  string
	Proxy      bool
}

type connectionStatus struct {
	Connected    *rpcConnection
	Disconnected bool
}

// ConnectionOptions configures how the client connects to the broker.
type ConnectionOptions struct {
	DialOptions []DialOption // Optional gRPC dial options.
	TLSConfig   *tls.Config  // TLS configuration (required when UseTLS is true).
	UseTLS      bool         // Enable TLS/mTLS for the connection.
	APIKey      string       // API key for auth (enables TLS with system roots).
}

type connectionManager struct {
	connections       map[brokerAddress]*connectionStatus
	connectionOptions ConnectionOptions
	connectionsMutex  sync.Mutex
}

// NewConnectionManager creates a new ConnectionManager.
func newConnectionManager(options ConnectionOptions) *connectionManager {
	return &connectionManager{
		connections:       make(map[brokerAddress]*connectionStatus),
		connectionOptions: options,
	}
}

func (cm *connectionManager) getConnection(brokerURL, connectURL string) (*rpcConnection, error) {
	cm.connectionsMutex.Lock()
	defer cm.connectionsMutex.Unlock()

	proxy := brokerURL != connectURL
	broker := brokerAddress{
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
