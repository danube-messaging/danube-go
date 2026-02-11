package integration_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	danube "github.com/danube-messaging/danube-go"
)

// runPartitionedBasic creates a partitioned producer (3 partitions), sends 3 messages,
// and verifies the consumer receives all of them with partition coverage.
func runPartitionedBasic(t *testing.T, topicPrefix string, subType danube.SubType) {
	t.Helper()
	client := setupClient(t)
	topic := uniqueTopic(topicPrefix)
	ctx := context.Background()
	partitions := int32(3)

	// Partitioned producer
	producer, err := client.NewProducer().
		WithTopic(topic).
		WithName("producer_part_basic").
		WithPartitions(partitions).
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
		WithConsumerName("cons_part_basic").
		WithSubscription("sub_part_basic").
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

	expected := []string{"Hello Danube 1", "Hello Danube 2", "Hello Danube 3"}
	for _, body := range expected {
		if _, err := producer.Send(ctx, []byte(body), nil); err != nil {
			t.Fatalf("failed to send message: %v", err)
		}
	}

	messages := receiveMessages(t, msgChan, len(expected), 10*time.Second)

	// Verify all payloads received
	received := make(map[string]bool)
	partsSeen := make(map[string]bool)
	for _, msg := range messages {
		received[string(msg.GetPayload())] = true
		partsSeen[msg.GetMsgId().GetTopicName()] = true
		if _, err := consumer.Ack(ctx, msg); err != nil {
			t.Fatalf("failed to ack: %v", err)
		}
	}

	for _, body := range expected {
		if !received[body] {
			t.Fatalf("missing message %q", body)
		}
	}

	// Verify partition coverage
	for i := int32(0); i < partitions; i++ {
		partName := topic + "-part-" + itoa(int(i))
		if !partsSeen[partName] {
			t.Errorf("missing partition %s", partName)
		}
	}
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}

// TestPartitionedBasicExclusive verifies a single Exclusive consumer transparently
// merges streams from all 3 partitions.
func TestPartitionedBasicExclusive(t *testing.T) {
	runPartitionedBasic(t, "/default/part_basic_excl", danube.Exclusive)
}

// TestPartitionedBasicShared verifies a single Shared consumer receives all messages
// from all 3 partitions.
func TestPartitionedBasicShared(t *testing.T) {
	runPartitionedBasic(t, "/default/part_basic_shared", danube.Shared)
}
