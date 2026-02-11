package danube

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// DanubeClient is the main client for interacting with the Danube messaging system.
// It provides methods to create producers and consumers, perform topic lookups, and retrieve schema information.
type DanubeClient struct {
	URI                string
	connectionManager  *connectionManager
	lookupService      *lookupService
	healthCheckService *healthCheckService
	authService        *AuthService
}

// NewClient initializes a new DanubeClientBuilder. The builder pattern allows for configuring and constructing
// a DanubeClient instance with optional settings and options.
//
// Returns:
// - *DanubeClientBuilder: A new instance of DanubeClientBuilder for configuring and building a DanubeClient.
func NewClient() *DanubeClientBuilder {
	return &DanubeClientBuilder{}
}

func newDanubeClient(builder DanubeClientBuilder) (*DanubeClient, error) {
	connectionManager := newConnectionManager(builder.connectionOptions)
	authService := newAuthService(connectionManager)

	if builder.connectionOptions.APIKey != "" {
		if _, err := authService.authenticateClient(context.Background(), builder.URI, builder.connectionOptions.APIKey); err != nil {
			return nil, err
		}
	}

	lookupService := NewLookupService(connectionManager, authService)
	healthCheckService := newHealthCheckService(connectionManager, authService)

	return &DanubeClient{
		URI:                builder.URI,
		connectionManager:  connectionManager,
		lookupService:      lookupService,
		healthCheckService: healthCheckService,
		authService:        authService,
	}, nil
}

// NewProducer returns a new ProducerBuilder, which is used to configure and create a Producer instance.
func (dc *DanubeClient) NewProducer() *ProducerBuilder {
	return newProducerBuilder(dc)
}

// NewConsumer returns a new ConsumerBuilder, which is used to configure and create a Consumer instance.
func (dc *DanubeClient) NewConsumer() *ConsumerBuilder {
	return newConsumerBuilder(dc)
}

// LookupTopic retrieves the address of the broker responsible for a specified topic.
//
// Parameters:
// - ctx: The context for managing the lookup operation.
// - addr: The address of the lookup service.
// - topic: The name of the topic to look up.
//
// Returns:
// - *LookupResult: The result of the lookup operation, containing broker address and other details.
// - error: An error if the lookup fails or other issues occur.
func (dc *DanubeClient) LookupTopic(ctx context.Context, addr string, topic string) (*LookupResult, error) {
	return dc.lookupService.lookupTopic(ctx, addr, topic)
}

// Schema returns a SchemaRegistryClient for schema operations.
func (dc *DanubeClient) Schema() *SchemaRegistryClient {
	return newSchemaRegistryClient(dc.connectionManager, dc.authService, dc.URI)
}

// DanubeClientBuilder is used for configuring and creating a DanubeClient instance. It provides methods for setting
// various options, including the service URL, connection options, and logger.
//
// Fields:
// - URI: The base URI for the Danube service. This is required for constructing the client.
// - ConnectionOptions: Optional connection settings for configuring how the client connects to the service.
type DanubeClientBuilder struct {
	URI               string
	connectionOptions ConnectionOptions
}

// ServiceURL sets the base URI for the Danube service in the builder.
//
// Parameters:
// - url: The base URI to use for connecting to the Danube service.
//
// Returns:
// - *DanubeClientBuilder: The updated builder instance with the new service URL.
func (b *DanubeClientBuilder) ServiceURL(url string) *DanubeClientBuilder {
	b.URI = url
	return b
}

// WithConnectionOptions sets connection settings for the client in the builder.
func (b *DanubeClientBuilder) WithConnectionOptions(options ConnectionOptions) *DanubeClientBuilder {
	b.connectionOptions = options
	return b
}

// WithDialOptions appends gRPC dial options to the connection options.
func (b *DanubeClientBuilder) WithDialOptions(options ...DialOption) *DanubeClientBuilder {
	b.connectionOptions.DialOptions = append(b.connectionOptions.DialOptions, options...)
	return b
}

// WithTLS enables TLS using a custom CA certificate.
func (b *DanubeClientBuilder) WithTLS(caCertPath string) (*DanubeClientBuilder, error) {
	caData, err := os.ReadFile(caCertPath)
	if err != nil {
		return b, err
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caData) {
		return b, fmt.Errorf("unable to parse CA certificate")
	}
	b.connectionOptions.TLSConfig = &tls.Config{RootCAs: caPool}
	b.connectionOptions.UseTLS = true
	return b, nil
}

// WithMTLS enables mutual TLS using CA, client certificate, and client key.
func (b *DanubeClientBuilder) WithMTLS(caCertPath, clientCertPath, clientKeyPath string) (*DanubeClientBuilder, error) {
	caData, err := os.ReadFile(caCertPath)
	if err != nil {
		return b, err
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caData) {
		return b, fmt.Errorf("unable to parse CA certificate")
	}

	certData, err := os.ReadFile(clientCertPath)
	if err != nil {
		return b, err
	}
	keyData, err := os.ReadFile(clientKeyPath)
	if err != nil {
		return b, err
	}

	clientCert, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return b, err
	}

	b.connectionOptions.TLSConfig = &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientCert},
	}
	b.connectionOptions.UseTLS = true
	return b, nil
}

// WithAPIKey configures API key authentication and enables TLS with system roots.
func (b *DanubeClientBuilder) WithAPIKey(apiKey string) *DanubeClientBuilder {
	b.connectionOptions.APIKey = apiKey
	b.connectionOptions.UseTLS = true
	if b.connectionOptions.TLSConfig == nil {
		b.connectionOptions.TLSConfig = &tls.Config{}
	}
	return b
}

// Build constructs and returns a DanubeClient instance based on the configuration specified in the builder.
//
// Returns:
// - *DanubeClient: A new instance of DanubeClient configured with the specified options.
func (b *DanubeClientBuilder) Build() (*DanubeClient, error) {
	return newDanubeClient(*b)
}
