package integration_tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	danube "github.com/danube-messaging/danube-go"
	"github.com/danube-messaging/danube-go/proto"
)

// runReliableNack creates a reliable-dispatch producer, sends one message,
// nacks it (triggering redelivery), receives the redelivered message, and
// acks it to confirm the cycle completes.
func runReliableNack(t *testing.T, topicPrefix string, subType danube.SubType) {
	t.Helper()
	client := setupClient(t)
	topic := uniqueTopic(topicPrefix)
	ctx := context.Background()

	// Producer with reliable dispatch
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_reliable_nack").
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
		WithConsumerName("cons_rel_nack").
		WithSubscription("rel_sub_nack").
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

	payload := []byte("nack-redelivery-test")

	if _, err := producer.Send(ctx, payload, nil); err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	// Receive the first delivery
	var firstMsg *proto.StreamMessage
	select {
	case msg, ok := <-msgChan:
		if !ok {
			t.Fatal("channel closed before first delivery")
		}
		firstMsg = msg
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}

	if !bytes.Equal(firstMsg.GetPayload(), payload) {
		t.Fatalf("first delivery payload mismatch: got %q, want %q", string(firstMsg.GetPayload()), string(payload))
	}

	// Nack the message with zero delay to trigger immediate redelivery
	delayMs := uint64(0)
	reason := "testing nack redelivery"
	if _, err := consumer.Nack(ctx, firstMsg, &delayMs, &reason); err != nil {
		t.Fatalf("failed to nack message: %v", err)
	}

	// Receive the redelivered message
	select {
	case msg, ok := <-msgChan:
		if !ok {
			t.Fatal("channel closed before redelivery")
		}
		if !bytes.Equal(msg.GetPayload(), payload) {
			t.Fatalf("redelivered payload mismatch: got %q, want %q", string(msg.GetPayload()), string(payload))
		}
		if msg.GetMsgId().GetTopicOffset() != firstMsg.GetMsgId().GetTopicOffset() {
			t.Fatalf("redelivered message has different offset: got %d, want %d",
				msg.GetMsgId().GetTopicOffset(), firstMsg.GetMsgId().GetTopicOffset())
		}
		// Ack the redelivered message to complete the cycle
		if _, err := consumer.Ack(ctx, msg); err != nil {
			t.Fatalf("failed to ack redelivered message: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for redelivered message after nack")
	}
}

// TestReliableNackExclusive validates that nacking a message on a reliable
// Exclusive subscription causes the broker to redeliver the same message.
func TestReliableNackExclusive(t *testing.T) {
	runReliableNack(t, "/default/reliable_nack_exclusive", danube.Exclusive)
}

// TestReliableNackShared validates that nacking a message on a reliable
// Shared subscription causes the broker to redeliver the same message.
func TestReliableNackShared(t *testing.T) {
	runReliableNack(t, "/default/reliable_nack_shared", danube.Shared)
}
