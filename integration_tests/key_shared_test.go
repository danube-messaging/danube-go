package integration_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	danube "github.com/danube-messaging/danube-go"
	"github.com/danube-messaging/danube-go/proto"
)

// TestKeySharedBasic verifies that Key-Shared dispatch routes messages
// with the same routing key to the same consumer consistently.
func TestKeySharedBasic(t *testing.T) {
	client := setupClient(t)
	topic := uniqueTopic("/default/key_shared_basic")
	ctx := context.Background()

	// Producer with reliable dispatch
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_ks_basic").
		WithDispatchStrategy(danube.NewReliableDispatchStrategy()).
		Build()
	if err != nil {
		t.Fatalf("failed to build producer: %v", err)
	}
	if err := producer.Create(ctx); err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	// Two consumers sharing the same Key-Shared subscription
	consumer1, err := client.NewConsumer().
		WithTopic(topic).
		WithConsumerName("cons_ks_1").
		WithSubscription("ks_sub_basic").
		WithSubscriptionType(danube.KeyShared).
		Build()
	if err != nil {
		t.Fatalf("failed to build consumer1: %v", err)
	}
	if err := consumer1.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe consumer1: %v", err)
	}
	defer consumer1.Close()

	consumer2, err := client.NewConsumer().
		WithTopic(topic).
		WithConsumerName("cons_ks_2").
		WithSubscription("ks_sub_basic").
		WithSubscriptionType(danube.KeyShared).
		Build()
	if err != nil {
		t.Fatalf("failed to build consumer2: %v", err)
	}
	if err := consumer2.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe consumer2: %v", err)
	}
	defer consumer2.Close()

	msgChan1, err := consumer1.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to receive consumer1: %v", err)
	}
	msgChan2, err := consumer2.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to receive consumer2: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Send messages with two different keys
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		key := "keyA"
		if i%2 == 1 {
			key = "keyB"
		}
		payload := fmt.Sprintf("msg-%d-key-%s", i, key)
		if _, err := producer.SendWithKey(ctx, []byte(payload), nil, key); err != nil {
			t.Fatalf("failed to send message %d: %v", i, err)
		}
	}

	// Collect all messages from both consumers
	allMessages := collectMessages(t, msgChan1, msgChan2, messageCount, 15*time.Second)

	// Ack all messages
	for _, entry := range allMessages {
		var ackErr error
		if entry.consumer == 1 {
			_, ackErr = consumer1.Ack(ctx, entry.msg)
		} else {
			_, ackErr = consumer2.Ack(ctx, entry.msg)
		}
		if ackErr != nil {
			t.Fatalf("failed to ack message: %v", ackErr)
		}
	}

	// Verify: all messages with the same key went to the same consumer
	keyToConsumer := make(map[string]int)
	for _, entry := range allMessages {
		key := entry.msg.GetRoutingKey()
		if prev, ok := keyToConsumer[key]; ok {
			if prev != entry.consumer {
				t.Fatalf("key %q was routed to consumer %d and %d — expected same consumer", key, prev, entry.consumer)
			}
		} else {
			keyToConsumer[key] = entry.consumer
		}
	}

	if len(allMessages) != messageCount {
		t.Fatalf("expected %d messages, got %d", messageCount, len(allMessages))
	}

	t.Logf("Key-Shared basic test passed: %d messages, key distribution: %v", len(allMessages), keyToConsumer)
}

// TestKeySharedFiltering verifies that key filters restrict which messages
// a consumer receives in a Key-Shared subscription.
func TestKeySharedFiltering(t *testing.T) {
	client := setupClient(t)
	topic := uniqueTopic("/default/key_shared_filter")
	ctx := context.Background()

	// Producer with reliable dispatch
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_ks_filter").
		WithDispatchStrategy(danube.NewReliableDispatchStrategy()).
		Build()
	if err != nil {
		t.Fatalf("failed to build producer: %v", err)
	}
	if err := producer.Create(ctx); err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	// Consumer with key filter: only receives "payment" keys
	filteredConsumer, err := client.NewConsumer().
		WithTopic(topic).
		WithConsumerName("cons_ks_filtered").
		WithSubscription("ks_sub_filter").
		WithSubscriptionType(danube.KeyShared).
		WithKeyFilter("payment").
		Build()
	if err != nil {
		t.Fatalf("failed to build filtered consumer: %v", err)
	}
	if err := filteredConsumer.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe filtered consumer: %v", err)
	}
	defer filteredConsumer.Close()

	// Second consumer with no filter: gets everything else
	catchAllConsumer, err := client.NewConsumer().
		WithTopic(topic).
		WithConsumerName("cons_ks_catchall").
		WithSubscription("ks_sub_filter").
		WithSubscriptionType(danube.KeyShared).
		Build()
	if err != nil {
		t.Fatalf("failed to build catch-all consumer: %v", err)
	}
	if err := catchAllConsumer.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe catch-all consumer: %v", err)
	}
	defer catchAllConsumer.Close()

	filteredChan, err := filteredConsumer.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to receive filtered: %v", err)
	}
	catchAllChan, err := catchAllConsumer.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to receive catch-all: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Send messages with different keys
	keys := []string{"payment", "shipping", "payment", "invoice", "payment"}
	for i, key := range keys {
		payload := fmt.Sprintf("event-%d-%s", i, key)
		if _, err := producer.SendWithKey(ctx, []byte(payload), nil, key); err != nil {
			t.Fatalf("failed to send message %d: %v", i, err)
		}
	}

	// Collect all messages
	allMessages := collectMessages(t, filteredChan, catchAllChan, len(keys), 15*time.Second)

	// Ack all
	for _, entry := range allMessages {
		var ackErr error
		if entry.consumer == 1 {
			_, ackErr = filteredConsumer.Ack(ctx, entry.msg)
		} else {
			_, ackErr = catchAllConsumer.Ack(ctx, entry.msg)
		}
		if ackErr != nil {
			t.Fatalf("failed to ack: %v", ackErr)
		}
	}

	// Verify: filtered consumer should only have "payment" messages
	paymentCount := 0
	for _, entry := range allMessages {
		key := entry.msg.GetRoutingKey()
		if entry.consumer == 1 {
			if key != "payment" {
				t.Fatalf("filtered consumer received key %q, expected only 'payment'", key)
			}
			paymentCount++
		}
	}

	// We sent 3 "payment" messages
	if paymentCount != 3 {
		t.Fatalf("expected 3 payment messages on filtered consumer, got %d", paymentCount)
	}

	if len(allMessages) != len(keys) {
		t.Fatalf("expected %d total messages, got %d", len(keys), len(allMessages))
	}

	t.Logf("Key-Shared filtering test passed: filtered consumer got %d/3 payment messages", paymentCount)
}

type messageEntry struct {
	msg      *proto.StreamMessage
	consumer int // 1 or 2
}

// collectMessages reads from two consumer channels until totalExpected messages
// are collected or timeout is reached.
func collectMessages(
	t *testing.T,
	ch1 <-chan *proto.StreamMessage,
	ch2 <-chan *proto.StreamMessage,
	totalExpected int,
	timeout time.Duration,
) []messageEntry {
	t.Helper()
	var entries []messageEntry
	deadline := time.After(timeout)

	for len(entries) < totalExpected {
		select {
		case msg, ok := <-ch1:
			if !ok {
				t.Fatalf("channel 1 closed after %d messages", len(entries))
			}
			entries = append(entries, messageEntry{msg: msg, consumer: 1})
		case msg, ok := <-ch2:
			if !ok {
				t.Fatalf("channel 2 closed after %d messages", len(entries))
			}
			entries = append(entries, messageEntry{msg: msg, consumer: 2})
		case <-deadline:
			t.Fatalf("timeout: collected only %d/%d messages", len(entries), totalExpected)
		}
	}
	return entries
}
