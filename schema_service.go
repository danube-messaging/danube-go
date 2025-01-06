package danube

import (
	"context"
	"errors"

	"github.com/danube-messaging/danube-go/proto" // Path to your generated proto package
)

// SchemaService is used to interact with the schema service
type schemaService struct {
	cnxManager *connectionManager
	requestID  uint64
}

// NewSchemaService creates a new instance of SchemaService
func newSchemaService(cnxManager *connectionManager) *schemaService {
	return &schemaService{
		cnxManager: cnxManager,
		requestID:  0,
	}
}

// GetSchema retrieves the schema for the given topic
func (ss *schemaService) getSchema(ctx context.Context, addr string, topic string) (*Schema, error) {
	conn, err := ss.cnxManager.getConnection(addr, addr)
	if err != nil {
		return nil, err
	}

	client := proto.NewDiscoveryClient(conn.grpcConn)

	schemaRequest := &proto.SchemaRequest{
		RequestId: ss.requestID,
		Topic:     topic,
	}

	response, err := client.GetSchema(ctx, schemaRequest)
	if err != nil {
		return nil, err
	}

	schemaResponse := response.GetSchema()
	if schemaResponse == nil {
		return nil, errors.New("schema response is nil")
	}

	// Convert ProtoSchema to Schema
	schema, err := FromProtoSchema(schemaResponse)
	if err != nil {
		return nil, err
	}

	return schema, nil
}
