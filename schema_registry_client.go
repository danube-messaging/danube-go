package danube

import (
	"context"
	"fmt"

	"github.com/danube-messaging/danube-go/proto"
)

// SchemaRegistryClient provides access to the schema registry APIs.
type SchemaRegistryClient struct {
	cnxManager  *connectionManager
	authService *AuthService
	uri         string
}

func newSchemaRegistryClient(cnxManager *connectionManager, authService *AuthService, uri string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		cnxManager:  cnxManager,
		authService: authService,
		uri:         uri,
	}
}

func (c *SchemaRegistryClient) prepare(ctx context.Context, addr string) (context.Context, proto.SchemaRegistryClient, error) {
	conn, err := c.cnxManager.getConnection(addr, addr)
	if err != nil {
		return ctx, nil, err
	}
	ctxWithAuth, err := c.authService.attachTokenIfNeeded(ctx, c.cnxManager.connectionOptions.APIKey, addr)
	if err != nil {
		return ctx, nil, err
	}
	return ctxWithAuth, proto.NewSchemaRegistryClient(conn.grpcConn), nil
}

// RegisterSchema returns a builder for schema registration.
func (c *SchemaRegistryClient) RegisterSchema(subject string) *SchemaRegistrationBuilder {
	return &SchemaRegistrationBuilder{client: c, subject: subject}
}

// GetSchemaByID fetches schema information for a schema ID (latest version).
func (c *SchemaRegistryClient) GetSchemaByID(ctx context.Context, schemaID uint64) (SchemaInfo, error) {
	return c.GetSchemaVersion(ctx, schemaID, nil)
}

// GetSchemaVersion fetches a specific version for a schema ID.
func (c *SchemaRegistryClient) GetSchemaVersion(ctx context.Context, schemaID uint64, version *uint32) (SchemaInfo, error) {
	ctxWithAuth, client, err := c.prepare(ctx, c.uri)
	if err != nil {
		return SchemaInfo{}, err
	}
	resp, err := client.GetSchema(ctxWithAuth, &proto.GetSchemaRequest{SchemaId: schemaID, Version: version})
	if err != nil {
		return SchemaInfo{}, err
	}
	return schemaInfoFromProto(resp), nil
}

// GetLatestSchema fetches the latest schema for a subject.
func (c *SchemaRegistryClient) GetLatestSchema(ctx context.Context, subject string) (SchemaInfo, error) {
	ctxWithAuth, client, err := c.prepare(ctx, c.uri)
	if err != nil {
		return SchemaInfo{}, err
	}
	resp, err := client.GetLatestSchema(ctxWithAuth, &proto.GetLatestSchemaRequest{Subject: subject})
	if err != nil {
		return SchemaInfo{}, err
	}
	return schemaInfoFromProto(resp), nil
}

// ListVersions returns all versions for a subject.
func (c *SchemaRegistryClient) ListVersions(ctx context.Context, subject string) ([]uint32, error) {
	ctxWithAuth, client, err := c.prepare(ctx, c.uri)
	if err != nil {
		return nil, err
	}
	resp, err := client.ListVersions(ctxWithAuth, &proto.ListVersionsRequest{Subject: subject})
	if err != nil {
		return nil, err
	}
	versions := make([]uint32, 0, len(resp.GetVersions()))
	for _, v := range resp.GetVersions() {
		versions = append(versions, v.GetVersion())
	}
	return versions, nil
}

// CheckCompatibility validates new schema data against existing versions.
func (c *SchemaRegistryClient) CheckCompatibility(ctx context.Context, subject string, schemaData []byte, schemaType SchemaType, mode *CompatibilityMode) (*proto.CheckCompatibilityResponse, error) {
	ctxWithAuth, client, err := c.prepare(ctx, c.uri)
	if err != nil {
		return nil, err
	}

	var compatibilityMode *string
	if mode != nil {
		modeStr := mode.AsString()
		compatibilityMode = &modeStr
	}

	resp, err := client.CheckCompatibility(ctxWithAuth, &proto.CheckCompatibilityRequest{
		Subject:             subject,
		NewSchemaDefinition: schemaData,
		SchemaType:          schemaType.AsString(),
		CompatibilityMode:   compatibilityMode,
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// SetCompatibilityMode sets the compatibility mode for a subject.
func (c *SchemaRegistryClient) SetCompatibilityMode(ctx context.Context, subject string, mode CompatibilityMode) (*proto.SetCompatibilityModeResponse, error) {
	ctxWithAuth, client, err := c.prepare(ctx, c.uri)
	if err != nil {
		return nil, err
	}
	resp, err := client.SetCompatibilityMode(ctxWithAuth, &proto.SetCompatibilityModeRequest{
		Subject:           subject,
		CompatibilityMode: mode.AsString(),
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteSchemaVersion removes a specific schema version.
func (c *SchemaRegistryClient) DeleteSchemaVersion(ctx context.Context, subject string, version uint32) (*proto.DeleteSchemaVersionResponse, error) {
	ctxWithAuth, client, err := c.prepare(ctx, c.uri)
	if err != nil {
		return nil, err
	}
	resp, err := client.DeleteSchemaVersion(ctxWithAuth, &proto.DeleteSchemaVersionRequest{
		Subject: subject,
		Version: version,
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ConfigureTopicSchema assigns a schema subject and validation policy to a topic (admin-only).
func (c *SchemaRegistryClient) ConfigureTopicSchema(ctx context.Context, topicName, schemaSubject, validationPolicy string, enablePayloadValidation bool) (*proto.ConfigureTopicSchemaResponse, error) {
	ctxWithAuth, client, err := c.prepare(ctx, c.uri)
	if err != nil {
		return nil, err
	}
	resp, err := client.ConfigureTopicSchema(ctxWithAuth, &proto.ConfigureTopicSchemaRequest{
		TopicName:               topicName,
		SchemaSubject:           schemaSubject,
		ValidationPolicy:        validationPolicy,
		EnablePayloadValidation: enablePayloadValidation,
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateTopicValidationPolicy updates validation policy for a topic (admin-only).
func (c *SchemaRegistryClient) UpdateTopicValidationPolicy(ctx context.Context, topicName, validationPolicy string, enablePayloadValidation bool) (*proto.UpdateTopicValidationPolicyResponse, error) {
	ctxWithAuth, client, err := c.prepare(ctx, c.uri)
	if err != nil {
		return nil, err
	}
	resp, err := client.UpdateTopicValidationPolicy(ctxWithAuth, &proto.UpdateTopicValidationPolicyRequest{
		TopicName:               topicName,
		ValidationPolicy:        validationPolicy,
		EnablePayloadValidation: enablePayloadValidation,
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetTopicSchemaConfig returns schema configuration for a topic.
func (c *SchemaRegistryClient) GetTopicSchemaConfig(ctx context.Context, topicName string) (*proto.GetTopicSchemaConfigResponse, error) {
	ctxWithAuth, client, err := c.prepare(ctx, c.uri)
	if err != nil {
		return nil, err
	}
	resp, err := client.GetTopicSchemaConfig(ctxWithAuth, &proto.GetTopicSchemaConfigRequest{TopicName: topicName})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// SchemaRegistrationBuilder provides a fluent API for schema registration.
type SchemaRegistrationBuilder struct {
	client      *SchemaRegistryClient
	subject     string
	schemaType  *SchemaType
	schemaData  []byte
	description string
	createdBy   string
	tags        []string
}

func (b *SchemaRegistrationBuilder) WithType(schemaType SchemaType) *SchemaRegistrationBuilder {
	b.schemaType = &schemaType
	return b
}

func (b *SchemaRegistrationBuilder) WithSchemaData(data []byte) *SchemaRegistrationBuilder {
	b.schemaData = data
	return b
}

func (b *SchemaRegistrationBuilder) WithDescription(description string) *SchemaRegistrationBuilder {
	b.description = description
	return b
}

func (b *SchemaRegistrationBuilder) WithCreatedBy(createdBy string) *SchemaRegistrationBuilder {
	b.createdBy = createdBy
	return b
}

func (b *SchemaRegistrationBuilder) WithTags(tags []string) *SchemaRegistrationBuilder {
	b.tags = tags
	return b
}

func (b *SchemaRegistrationBuilder) Execute(ctx context.Context) (uint64, error) {
	if b.schemaType == nil {
		return 0, fmt.Errorf("schema type is required")
	}
	if len(b.schemaData) == 0 {
		return 0, fmt.Errorf("schema data is required")
	}
	createdBy := b.createdBy
	if createdBy == "" {
		createdBy = "danube-go"
	}
	ctxWithAuth, client, err := b.client.prepare(ctx, b.client.uri)
	if err != nil {
		return 0, err
	}
	resp, err := client.RegisterSchema(ctxWithAuth, &proto.RegisterSchemaRequest{
		Subject:          b.subject,
		SchemaType:       b.schemaType.AsString(),
		SchemaDefinition: b.schemaData,
		Description:      b.description,
		CreatedBy:        createdBy,
		Tags:             b.tags,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetSchemaId(), nil
}
