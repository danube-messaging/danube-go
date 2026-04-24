package integration_tests

import (
	"context"
	"fmt"
	"sync"
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

	// Collect messages from both consumers, acking inline.
	// Key-Shared dispatch has per-key in-flight limits — the broker
	// won't send the next message for the same key until the current one
	// is acked. We must ack as we go to avoid deadlocking.
	type entry struct {
		msg      *proto.StreamMessage
		consumer int
	}
	var entries []entry
	deadline := time.After(15 * time.Second)

	for len(entries) < messageCount {
		select {
		case msg, ok := <-msgChan1:
			if !ok {
				t.Fatalf("channel 1 closed after %d messages", len(entries))
			}
			if _, err := consumer1.Ack(ctx, msg); err != nil {
				t.Fatalf("failed to ack on consumer1: %v", err)
			}
			entries = append(entries, entry{msg: msg, consumer: 1})
		case msg, ok := <-msgChan2:
			if !ok {
				t.Fatalf("channel 2 closed after %d messages", len(entries))
			}
			if _, err := consumer2.Ack(ctx, msg); err != nil {
				t.Fatalf("failed to ack on consumer2: %v", err)
			}
			entries = append(entries, entry{msg: msg, consumer: 2})
		case <-deadline:
			t.Fatalf("timeout: collected only %d/%d messages", len(entries), messageCount)
		}
	}

	// Verify: all messages with the same key went to the same consumer
	keyToConsumer := make(map[string]int)
	for _, e := range entries {
		key := e.msg.GetRoutingKey()
		if prev, ok := keyToConsumer[key]; ok {
			if prev != e.consumer {
				t.Fatalf("key %q was routed to consumer %d and %d — expected same consumer", key, prev, e.consumer)
			}
		} else {
			keyToConsumer[key] = e.consumer
		}
	}

	if len(entries) != messageCount {
		t.Fatalf("expected %d messages, got %d", messageCount, len(entries))
	}

	t.Logf("Key-Shared basic test passed: %d messages, key distribution: %v", len(entries), keyToConsumer)
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

	// Collect all messages, acking inline to unblock per-key dispatch
	var mu sync.Mutex
	var filteredKeys []string
	var allCount int
	deadline := time.After(15 * time.Second)

	for allCount < len(keys) {
		select {
		case msg, ok := <-filteredChan:
			if !ok {
				t.Fatalf("filtered channel closed after %d messages", allCount)
			}
			if _, err := filteredConsumer.Ack(ctx, msg); err != nil {
				t.Fatalf("failed to ack on filtered consumer: %v", err)
			}
			mu.Lock()
			filteredKeys = append(filteredKeys, msg.GetRoutingKey())
			allCount++
			mu.Unlock()
		case msg, ok := <-catchAllChan:
			if !ok {
				t.Fatalf("catch-all channel closed after %d messages", allCount)
			}
			if _, err := catchAllConsumer.Ack(ctx, msg); err != nil {
				t.Fatalf("failed to ack on catch-all consumer: %v", err)
			}
			key := msg.GetRoutingKey()
			if key == "payment" {
				t.Fatalf("catch-all consumer received 'payment' key — should have gone to filtered consumer")
			}
			mu.Lock()
			allCount++
			mu.Unlock()
		case <-deadline:
			t.Fatalf("timeout: collected only %d/%d messages", allCount, len(keys))
		}
	}

	// Verify: filtered consumer should only have "payment" messages
	for _, k := range filteredKeys {
		if k != "payment" {
			t.Fatalf("filtered consumer received key %q, expected only 'payment'", k)
		}
	}

	// We sent 3 "payment" messages
	if len(filteredKeys) != 3 {
		t.Fatalf("expected 3 payment messages on filtered consumer, got %d", len(filteredKeys))
	}

	t.Logf("Key-Shared filtering test passed: filtered consumer got %d/3 payment messages", len(filteredKeys))
}
