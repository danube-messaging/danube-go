package integration_tests

import (
	"context"
	"testing"
	"time"

	danube "github.com/danube-messaging/danube-go"
)

// TestProducerPinToVersion registers V1 and V2, creates a producer pinned to V1,
// sends a message, and verifies the consumer receives it with schema_version=1.
func TestProducerPinToVersion(t *testing.T) {
	client := setupClient(t)
	topic := uniqueTopic("/default/pin_version")
	ctx := context.Background()
	schemaClient := client.Schema()

	subject := uniqueTopic("version-pin-go")

	// Register V1
	schemaV1 := `{"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}`
	if _, err := schemaClient.RegisterSchema(subject).
		WithType(danube.SchemaTypeJSONSchema).
		WithSchemaData([]byte(schemaV1)).
		Execute(ctx); err != nil {
		t.Fatalf("failed to register schema V1: %v", err)
	}

	// Register V2
	schemaV2 := `{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}, "required": ["id"]}`
	if _, err := schemaClient.RegisterSchema(subject).
		WithType(danube.SchemaTypeJSONSchema).
		WithSchemaData([]byte(schemaV2)).
		Execute(ctx); err != nil {
		t.Fatalf("failed to register schema V2: %v", err)
	}

	// Producer pinned to V1
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_v1_pinned").
		WithSchemaVersion(subject, 1).
		Build()
	if err != nil {
		t.Fatalf("failed to build producer: %v", err)
	}
	if err := producer.Create(ctx); err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	// Consumer
	consumer, err := client.NewConsumer().
		WithTopic(topic).
		WithConsumerName("consumer_version").
		WithSubscription("sub_version").
		WithSubscriptionType(danube.Exclusive).
		Build()
	if err != nil {
		t.Fatalf("failed to build consumer: %v", err)
	}
	if err := consumer.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer consumer.Close()

	msgChan, err := consumer.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to receive: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if _, err := producer.Send(ctx, []byte(`{"id": 123}`), nil); err != nil {
		t.Fatalf("failed to send: %v", err)
	}

	msg := receiveOne(t, msgChan, 5*time.Second)

	if msg.SchemaVersion == nil || msg.GetSchemaVersion() != 1 {
		t.Fatalf("expected schema_version=1, got %v", msg.SchemaVersion)
	}

	if _, err := consumer.Ack(ctx, msg); err != nil {
		t.Fatalf("failed to ack: %v", err)
	}
}

// TestProducerLatestVersion registers V1 and V2, creates a producer using
// WithSchemaSubject (no version pin), and verifies the consumer receives
// the message with schema_version=2 (the latest).
func TestProducerLatestVersion(t *testing.T) {
	client := setupClient(t)
	topic := uniqueTopic("/default/latest_version")
	ctx := context.Background()
	schemaClient := client.Schema()

	subject := uniqueTopic("latest-version-go")

	// Register V1
	schemaV1 := `{"type": "object", "properties": {"a": {"type": "string"}}}`
	if _, err := schemaClient.RegisterSchema(subject).
		WithType(danube.SchemaTypeJSONSchema).
		WithSchemaData([]byte(schemaV1)).
		Execute(ctx); err != nil {
		t.Fatalf("failed to register schema V1: %v", err)
	}

	// Register V2
	schemaV2 := `{"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "integer"}}}`
	if _, err := schemaClient.RegisterSchema(subject).
		WithType(danube.SchemaTypeJSONSchema).
		WithSchemaData([]byte(schemaV2)).
		Execute(ctx); err != nil {
		t.Fatalf("failed to register schema V2: %v", err)
	}

	// Producer without version pin (should use latest V2)
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_latest").
		WithSchemaSubject(subject).
		Build()
	if err != nil {
		t.Fatalf("failed to build producer: %v", err)
	}
	if err := producer.Create(ctx); err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	// Consumer
	consumer, err := client.NewConsumer().
		WithTopic(topic).
		WithConsumerName("consumer_latest").
		WithSubscription("sub_latest").
		Build()
	if err != nil {
		t.Fatalf("failed to build consumer: %v", err)
	}
	if err := consumer.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer consumer.Close()

	msgChan, err := consumer.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to receive: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if _, err := producer.Send(ctx, []byte(`{"a": "test"}`), nil); err != nil {
		t.Fatalf("failed to send: %v", err)
	}

	msg := receiveOne(t, msgChan, 5*time.Second)

	if msg.SchemaVersion == nil || msg.GetSchemaVersion() != 2 {
		t.Fatalf("expected schema_version=2 (latest), got %v", msg.SchemaVersion)
	}

	if _, err := consumer.Ack(ctx, msg); err != nil {
		t.Fatalf("failed to ack: %v", err)
	}
}
