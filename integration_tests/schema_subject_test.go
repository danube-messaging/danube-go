package integration_tests

import (
	"context"
	"testing"

	danube "github.com/danube-messaging/danube-go"
)

// TestProducerWithRegisteredSchema registers a JSON schema, creates a producer
// referencing that subject, and sends a message successfully.
func TestProducerWithRegisteredSchema(t *testing.T) {
	client := setupClient(t)
	topic := uniqueTopic("/default/schema_registered")
	ctx := context.Background()
	schemaClient := client.Schema()

	jsonSchema := `{"type": "object", "properties": {"msg": {"type": "string"}}}`

	_, err := schemaClient.RegisterSchema("test-registered-schema-go").
		WithType(danube.SchemaTypeJSONSchema).
		WithSchemaData([]byte(jsonSchema)).
		Execute(ctx)
	if err != nil {
		t.Fatalf("failed to register schema: %v", err)
	}

	// Create producer with schema reference
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_registered").
		WithSchemaSubject("test-registered-schema-go").
		Build()
	if err != nil {
		t.Fatalf("failed to build producer: %v", err)
	}

	if err := producer.Create(ctx); err != nil {
		t.Fatalf("producer creation should succeed with registered schema: %v", err)
	}

	payload := `{"msg": "hello"}`
	if _, err := producer.Send(ctx, []byte(payload), nil); err != nil {
		t.Fatalf("message send should succeed: %v", err)
	}
}

// TestProducerWithoutSchema verifies that a producer without a schema can send
// arbitrary bytes (implicit Bytes schema).
func TestProducerWithoutSchema(t *testing.T) {
	client := setupClient(t)
	topic := uniqueTopic("/default/no_schema")
	ctx := context.Background()

	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_no_schema").
		Build()
	if err != nil {
		t.Fatalf("failed to build producer: %v", err)
	}

	if err := producer.Create(ctx); err != nil {
		t.Fatalf("producer creation should succeed without schema: %v", err)
	}

	if _, err := producer.Send(ctx, []byte("any bytes work"), nil); err != nil {
		t.Fatalf("message send should succeed: %v", err)
	}
}

// TestSchemaVersionEvolution registers V1 and V2 of a schema, then verifies
// the latest version returned by the registry is V2.
func TestSchemaVersionEvolution(t *testing.T) {
	client := setupClient(t)
	ctx := context.Background()
	schemaClient := client.Schema()

	subject := uniqueTopic("versioned-schema-go")

	// V1
	schemaV1 := `{"type": "object", "properties": {"name": {"type": "string"}}}`
	idV1, err := schemaClient.RegisterSchema(subject).
		WithType(danube.SchemaTypeJSONSchema).
		WithSchemaData([]byte(schemaV1)).
		Execute(ctx)
	if err != nil {
		t.Fatalf("failed to register schema V1: %v", err)
	}

	// V2 (add optional field)
	schemaV2 := `{"type": "object", "properties": {"name": {"type": "string"}, "email": {"type": "string"}}}`
	idV2, err := schemaClient.RegisterSchema(subject).
		WithType(danube.SchemaTypeJSONSchema).
		WithSchemaData([]byte(schemaV2)).
		Execute(ctx)
	if err != nil {
		t.Fatalf("failed to register schema V2: %v", err)
	}

	// Same subject → same schema_id
	if idV1 != idV2 {
		t.Fatalf("expected same schema_id for same subject, got V1=%d V2=%d", idV1, idV2)
	}

	// Latest should be V2
	latest, err := schemaClient.GetLatestSchema(ctx, subject)
	if err != nil {
		t.Fatalf("failed to get latest schema: %v", err)
	}
	if latest.Version != 2 {
		t.Fatalf("expected latest version 2, got %d", latest.Version)
	}

}
