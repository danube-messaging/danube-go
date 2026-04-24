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
//
// Both consumers use explicit filters so each key has exactly one eligible
// consumer, making routing deterministic regardless of hash ring layout.
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

	// Consumer A: only receives "payment" keys
	consumerA, err := client.NewConsumer().
		WithTopic(topic).
		WithConsumerName("cons_ks_payments").
		WithSubscription("ks_sub_filter").
		WithSubscriptionType(danube.KeyShared).
		WithKeyFilter("payment").
		Build()
	if err != nil {
		t.Fatalf("failed to build consumer A: %v", err)
	}
	if err := consumerA.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe consumer A: %v", err)
	}
	defer consumerA.Close()

	// Consumer B: receives "ship*" and "invoice" keys
	consumerB, err := client.NewConsumer().
		WithTopic(topic).
		WithConsumerName("cons_ks_logistics").
		WithSubscription("ks_sub_filter").
		WithSubscriptionType(danube.KeyShared).
		WithKeyFilter("ship*").
		WithKeyFilter("invoice").
		Build()
	if err != nil {
		t.Fatalf("failed to build consumer B: %v", err)
	}
	if err := consumerB.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe consumer B: %v", err)
	}
	defer consumerB.Close()

	chanA, err := consumerA.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to receive consumer A: %v", err)
	}
	chanB, err := consumerB.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to receive consumer B: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Send messages with different keys
	// "payment" → only A eligible, "shipping"/"invoice" → only B eligible
	keys := []string{"payment", "shipping", "payment", "invoice", "payment"}
	for i, key := range keys {
		payload := fmt.Sprintf("event-%d-%s", i, key)
		if _, err := producer.SendWithKey(ctx, []byte(payload), nil, key); err != nil {
			t.Fatalf("failed to send message %d: %v", i, err)
		}
	}

	// Collect all messages, acking inline to unblock per-key dispatch
	var mu sync.Mutex
	var keysOnA []string
	var keysOnB []string
	var allCount int
	deadline := time.After(15 * time.Second)

	for allCount < len(keys) {
		select {
		case msg, ok := <-chanA:
			if !ok {
				t.Fatalf("channel A closed after %d messages", allCount)
			}
			if _, err := consumerA.Ack(ctx, msg); err != nil {
				t.Fatalf("failed to ack on consumer A: %v", err)
			}
			mu.Lock()
			keysOnA = append(keysOnA, msg.GetRoutingKey())
			allCount++
			mu.Unlock()
		case msg, ok := <-chanB:
			if !ok {
				t.Fatalf("channel B closed after %d messages", allCount)
			}
			if _, err := consumerB.Ack(ctx, msg); err != nil {
				t.Fatalf("failed to ack on consumer B: %v", err)
			}
			mu.Lock()
			keysOnB = append(keysOnB, msg.GetRoutingKey())
			allCount++
			mu.Unlock()
		case <-deadline:
			t.Fatalf("timeout: collected only %d/%d messages", allCount, len(keys))
		}
	}

	// Verify: consumer A should only have "payment" keys
	for _, k := range keysOnA {
		if k != "payment" {
			t.Fatalf("consumer A (filter: payment) received key %q", k)
		}
	}

	// Verify: consumer B should only have "shipping" or "invoice" keys
	for _, k := range keysOnB {
		if k != "shipping" && k != "invoice" {
			t.Fatalf("consumer B (filter: ship*, invoice) received key %q", k)
		}
	}

	// We sent 3 "payment" + 1 "shipping" + 1 "invoice"
	if len(keysOnA) != 3 {
		t.Fatalf("expected 3 messages on consumer A, got %d", len(keysOnA))
	}
	if len(keysOnB) != 2 {
		t.Fatalf("expected 2 messages on consumer B, got %d", len(keysOnB))
	}

	t.Logf("Key-Shared filtering test passed: A got %d payment, B got %d logistics", len(keysOnA), len(keysOnB))
}
