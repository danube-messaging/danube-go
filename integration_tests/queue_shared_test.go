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

// TestQueueSharedRoundRobin validates queue semantics with Shared subscriptions.
// 3 consumers share the same subscription name; 36 messages are distributed
// across them approximately round-robin (each receives ~12).
func TestQueueSharedRoundRobin(t *testing.T) {
	client := setupClient(t)
	topic := uniqueTopic("/default/queue_shared")
	ctx := context.Background()

	// Producer
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_queue_shared").
		Build()
	if err != nil {
		t.Fatalf("failed to build producer: %v", err)
	}
	if err := producer.Create(ctx); err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	// 3 consumers, same subscription name
	subName := "queue-shared-sub"
	type consumerEntry struct {
		name    string
		cons    *danube.Consumer
		msgChan <-chan *proto.StreamMessage
	}

	var consumers []consumerEntry
	for i := 0; i < 3; i++ {
		cname := fmt.Sprintf("queue-cons-%d", i)
		cons, err := client.NewConsumer().
			WithTopic(topic).
			WithConsumerName(cname).
			WithSubscription(subName).
			WithSubscriptionType(danube.Shared).
			Build()
		if err != nil {
			t.Fatalf("failed to build consumer %d: %v", i, err)
		}
		if err := cons.Subscribe(ctx); err != nil {
			t.Fatalf("failed to subscribe consumer %d: %v", i, err)
		}
		defer cons.Close()
		msgChan, err := cons.Receive(ctx)
		if err != nil {
			t.Fatalf("failed to receive consumer %d: %v", i, err)
		}
		consumers = append(consumers, consumerEntry{name: cname, cons: cons, msgChan: msgChan})
	}

	time.Sleep(400 * time.Millisecond)

	// Collect receipts concurrently
	total := 36
	var mu sync.Mutex
	counts := make(map[string]int)
	received := 0

	done := make(chan struct{})

	for _, ce := range consumers {
		ce := ce
		go func() {
			for msg := range ce.msgChan {
				ce.cons.Ack(ctx, msg)
				mu.Lock()
				counts[ce.name]++
				received++
				if received >= total {
					mu.Unlock()
					select {
					case done <- struct{}{}:
					default:
					}
					return
				}
				mu.Unlock()
			}
		}()
	}

	// Send messages
	for i := 0; i < total; i++ {
		body := fmt.Sprintf("m%d", i)
		if _, err := producer.Send(ctx, []byte(body), nil); err != nil {
			t.Fatalf("failed to send message %d: %v", i, err)
		}
	}

	// Wait for all messages to be received
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		mu.Lock()
		t.Fatalf("timeout: received %d/%d messages", received, total)
		mu.Unlock()
	}

	// Assert near round-robin: each consumer should get total/3
	expected := total / 3
	for i := 0; i < 3; i++ {
		cname := fmt.Sprintf("queue-cons-%d", i)
		got := counts[cname]
		if got != expected {
			t.Errorf("%s received %d messages, expected %d", cname, got, expected)
		}
	}
}
