package integration_tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	danube "github.com/danube-messaging/danube-go"
	"github.com/danube-messaging/danube-go/proto"
)

const defaultBrokerURL = "http://127.0.0.1:6650"

func brokerURL() string {
	if url := os.Getenv("DANUBE_BROKER_URL"); url != "" {
		return url
	}
	return defaultBrokerURL
}

func setupClient(t *testing.T) *danube.DanubeClient {
	t.Helper()
	client, err := danube.NewClient().ServiceURL(brokerURL()).Build()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return client
}

func uniqueTopic(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixMilli())
}

// receiveMessages reads n messages from msgChan within the given timeout.
// It fails the test if the timeout is reached before all messages are received.
func receiveMessages(t *testing.T, msgChan <-chan *proto.StreamMessage, n int, timeout time.Duration) []*proto.StreamMessage {
	t.Helper()
	var messages []*proto.StreamMessage
	deadline := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				t.Fatalf("message channel closed after receiving %d/%d messages", len(messages), n)
			}
			messages = append(messages, msg)
		case <-deadline:
			t.Fatalf("timeout: received only %d/%d messages", len(messages), n)
		}
	}
	return messages
}

// receiveOne reads a single message from msgChan within the given timeout.
func receiveOne(t *testing.T, msgChan <-chan *proto.StreamMessage, timeout time.Duration) *proto.StreamMessage {
	t.Helper()
	msgs := receiveMessages(t, msgChan, 1, timeout)
	return msgs[0]
}

func ctxTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
