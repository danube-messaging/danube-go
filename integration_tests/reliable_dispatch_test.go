package integration_tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	danube "github.com/danube-messaging/danube-go"
)

// runReliableBasic creates a reliable-dispatch producer, sends 20 identical payloads,
// and verifies the consumer receives all of them with exact payload integrity.
func runReliableBasic(t *testing.T, topicPrefix string, subType danube.SubType) {
	t.Helper()
	client := setupClient(t)
	topic := uniqueTopic(topicPrefix)
	ctx := context.Background()

	// Producer with reliable dispatch
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_reliable_basic").
		WithDispatchStrategy(danube.NewReliableDispatchStrategy()).
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
		WithConsumerName("cons_rel_basic").
		WithSubscription("rel_sub_basic").
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

	time.Sleep(400 * time.Millisecond)

	// Generate a non-trivial payload (1 KB)
	blobData := bytes.Repeat([]byte("danube-reliable-test-payload!"), 37) // ~1 KB
	messageCount := 20

	for i := 0; i < messageCount; i++ {
		if _, err := producer.Send(ctx, blobData, nil); err != nil {
			t.Fatalf("failed to send message %d: %v", i, err)
		}
	}

	// For reliable dispatch the broker waits for an ack before sending the next
	// message, so we must ack inline during receive (matching Rust test behavior).
	deadline := time.After(15 * time.Second)
	for i := 0; i < messageCount; i++ {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				t.Fatalf("channel closed after %d/%d messages", i, messageCount)
			}
			if !bytes.Equal(msg.GetPayload(), blobData) {
				t.Fatalf("message %d payload mismatch: got %d bytes, want %d bytes", i, len(msg.GetPayload()), len(blobData))
			}
			if _, err := consumer.Ack(ctx, msg); err != nil {
				t.Fatalf("failed to ack message %d: %v", i, err)
			}
		case <-deadline:
			t.Fatalf("timeout: received only %d/%d messages", i, messageCount)
		}
	}
}

// TestReliableDispatchExclusive validates reliable dispatch with an Exclusive consumer:
// no drops, no corruption, acknowledgments succeed.
func TestReliableDispatchExclusive(t *testing.T) {
	runReliableBasic(t, "/default/reliable_basic_exclusive", danube.Exclusive)
}

// TestReliableDispatchShared validates reliable dispatch with a Shared consumer:
// consistent behavior under Shared subscription type.
func TestReliableDispatchShared(t *testing.T) {
	runReliableBasic(t, "/default/reliable_basic_shared", danube.Shared)
}
