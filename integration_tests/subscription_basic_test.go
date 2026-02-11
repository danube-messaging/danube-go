package integration_tests

import (
	"context"
	"testing"
	"time"

	danube "github.com/danube-messaging/danube-go"
)

// runBasicSubscription is a shared helper for basic subscription tests.
// It creates one producer and one consumer on a non-partitioned topic,
// sends a single message, and verifies the consumer receives and acks it.
func runBasicSubscription(t *testing.T, topicPrefix string, subType danube.SubType) {
	t.Helper()
	client := setupClient(t)
	topic := uniqueTopic(topicPrefix)
	ctx := context.Background()

	// Producer
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_basic").
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
		WithConsumerName("consumer_basic").
		WithSubscription("test_sub_basic").
		WithSubscriptionType(subType).
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

	time.Sleep(300 * time.Millisecond)

	payload := []byte("Hello Danube")
	if _, err := producer.Send(ctx, payload, nil); err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	msg := receiveOne(t, msgChan, 10*time.Second)

	if got := string(msg.GetPayload()); got != "Hello Danube" {
		t.Fatalf("payload mismatch: got %q, want %q", got, "Hello Danube")
	}

	if _, err := consumer.Ack(ctx, msg); err != nil {
		t.Fatalf("failed to ack: %v", err)
	}
}

// TestBasicSubscriptionShared validates baseline Shared subscription wiring:
// one producer, one consumer, send/receive/ack on a non-partitioned topic.
func TestBasicSubscriptionShared(t *testing.T) {
	runBasicSubscription(t, "/default/sub_basic_shared", danube.Shared)
}

// TestBasicSubscriptionExclusive validates baseline Exclusive subscription wiring:
// one producer, one consumer, send/receive/ack on a non-partitioned topic.
func TestBasicSubscriptionExclusive(t *testing.T) {
	runBasicSubscription(t, "/default/sub_basic_exclusive", danube.Exclusive)
}
