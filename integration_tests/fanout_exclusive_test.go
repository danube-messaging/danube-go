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

// TestFanoutExclusive validates pub-sub fan-out with Exclusive subscriptions.
// 3 consumers each with a unique subscription receive ALL messages (broadcast).
// 24 messages are sent; each consumer must receive all 24.
func TestFanoutExclusive(t *testing.T) {
	client := setupClient(t)
	topic := uniqueTopic("/default/fanout_exclusive")
	ctx := context.Background()

	// Producer
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_fanout_exclusive").
		Build()
	if err != nil {
		t.Fatalf("failed to build producer: %v", err)
	}
	if err := producer.Create(ctx); err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	// 3 consumers, each with unique Exclusive subscription
	type consumerEntry struct {
		name    string
		cons    *danube.Consumer
		msgChan <-chan *proto.StreamMessage
	}

	var consumers []consumerEntry
	for i := 0; i < 3; i++ {
		cname := fmt.Sprintf("fanout-cons-%d", i)
		subName := fmt.Sprintf("fanout-sub-%d", i)
		cons, err := client.NewConsumer().
			WithTopic(topic).
			WithConsumerName(cname).
			WithSubscription(subName).
			WithSubscriptionType(danube.Exclusive).
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

	// Collect per-consumer receipts concurrently
	total := 24
	expectedPayloads := make([]string, total)
	for i := 0; i < total; i++ {
		expectedPayloads[i] = fmt.Sprintf("m%d", i)
	}

	type receipt struct {
		consumerName string
		payload      string
	}
	receiptChan := make(chan receipt, total*3)

	var wg sync.WaitGroup
	for _, ce := range consumers {
		ce := ce
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range ce.msgChan {
				payload := string(msg.GetPayload())
				ce.cons.Ack(ctx, msg)
				receiptChan <- receipt{consumerName: ce.name, payload: payload}
			}
		}()
	}

	// Send messages
	for _, body := range expectedPayloads {
		if _, err := producer.Send(ctx, []byte(body), nil); err != nil {
			t.Fatalf("failed to send: %v", err)
		}
	}

	// Collect until each consumer has all messages
	perConsumer := make(map[string]map[string]bool)
	for i := 0; i < 3; i++ {
		perConsumer[fmt.Sprintf("fanout-cons-%d", i)] = make(map[string]bool)
	}

	deadline := time.After(20 * time.Second)
	allDone := false
	for !allDone {
		select {
		case r := <-receiptChan:
			perConsumer[r.consumerName][r.payload] = true
			allDone = true
			for _, payloads := range perConsumer {
				if len(payloads) < total {
					allDone = false
					break
				}
			}
		case <-deadline:
			for name, payloads := range perConsumer {
				t.Logf("%s received %d/%d", name, len(payloads), total)
			}
			t.Fatalf("timeout waiting for fan-out delivery")
		}
	}

	// Assert every consumer got every message
	for i := 0; i < 3; i++ {
		cname := fmt.Sprintf("fanout-cons-%d", i)
		got := perConsumer[cname]
		if len(got) != total {
			t.Fatalf("%s received %d messages, want %d", cname, len(got), total)
		}
		for _, body := range expectedPayloads {
			if !got[body] {
				t.Fatalf("%s missing message %s", cname, body)
			}
		}
	}
}
