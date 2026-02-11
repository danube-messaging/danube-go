package danube

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/danube-messaging/danube-go/proto"
)

// SubType is the type of subscription (e.g., EXCLUSIVE, SHARED, FAILOVER).
type SubType int

const (
	// Exclusive - only one consumer can subscribe to a specific subscription
	Exclusive SubType = iota
	//  Shared - multiple consumers can subscribe, messages are delivered round-robin
	Shared
	// FailOver - similar to exclusive subscriptions, but multiple consumers can subscribe, and one actively receives messages
	FailOver
)

// Consumer represents a message consumer that subscribes to a topic and receives messages.
// It handles communication with the message broker and manages the consumer's state.
type Consumer struct {
	mu               sync.Mutex
	client           *DanubeClient
	topicName        string                    // the name of the topic that the consumer subscribes to
	consumerName     string                    // the name assigned to the consumer instance
	consumers        map[string]*topicConsumer // the map between the partitioned topic name and the consumer instance
	subscription     string                    // the name of the subscription for the consumer
	subscriptionType SubType                   // the type of subscription (e.g., EXCLUSIVE, SHARED, FAILOVER)
	consumerOptions  ConsumerOptions           // configuration options for the consumer
	shutdown         *atomic.Bool
	receiveCancel    context.CancelFunc
	receiveWg        sync.WaitGroup
}

func newConsumer(
	client *DanubeClient,
	topicName, consumerName, subscription string,
	subType *SubType,
	options ConsumerOptions,
) *Consumer {
	var subscriptionType SubType
	if subType != nil {
		subscriptionType = *subType
	} else {
		subscriptionType = Shared
	}

	return &Consumer{
		client:           client,
		topicName:        topicName,
		consumerName:     consumerName,
		subscription:     subscription,
		subscriptionType: subscriptionType,
		consumerOptions:  options,
		shutdown:         &atomic.Bool{},
	}
}

// Subscribe initializes the subscription to the non-partitioned or partitioned topic and starts the health check service.
// It establishes a gRPC connection with the brokers and requests to subscribe to the topic.
//
// Parameters:
// - ctx: The context for managing the subscription lifecycle.
//
// Returns:
// - error: An error if the subscription fails or if initialization encounters issues.
func (c *Consumer) Subscribe(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get topic partitions
	partitions, err := c.client.lookupService.topicPartitions(ctx, c.client.URI, c.topicName)
	if err != nil {
		return err
	}

	// Initialize the consumers map
	consumers := make(map[string]*topicConsumer)

	// Channels to collect errors and results
	errChan := make(chan error, len(partitions))
	doneChan := make(chan struct{}, len(partitions))

	// Create and subscribe to topicConsumer
	for _, partition := range partitions {
		partition := partition
		go func() {
			defer func() { doneChan <- struct{}{} }()

			tc := newTopicConsumer(
				c.client,
				partition,
				c.consumerName,
				c.subscription,
				&c.subscriptionType,
				c.consumerOptions,
			)

			// Subscribe the topicConsumer and handle result
			if _, err := tc.subscribe(ctx); err != nil {
				errChan <- err
				return
			}

			consumers[partition] = &tc
		}()
	}

	// Wait for all goroutines to complete and check for errors
	for i := 0; i < len(partitions); i++ {
		select {
		case err := <-errChan:
			return err
		case <-doneChan:
			// Successful completion
		}
	}

	if len(consumers) == 0 {
		return fmt.Errorf("no partitions found")
	}

	c.consumers = consumers

	return nil
}

// Receive starts receiving messages from the subscribed partitioned or non-partitioned topic.
// It continuously polls for new messages and handles them as long as the stopSignal has not been set to true.
//
// Parameters:
// - ctx: The context for managing the receive operation.
//
// Returns:
// - StreamMessage channel for receiving messages from the broker.
// - error: An error if the receive client cannot be created or if other issues occur.
func (c *Consumer) Receive(ctx context.Context) (chan *proto.StreamMessage, error) {
	msgChan := make(chan *proto.StreamMessage, 100)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	c.receiveCancel = cancel

	retryManager := newRetryManager(c.consumerOptions.MaxRetries, c.consumerOptions.BaseBackoffMs, c.consumerOptions.MaxBackoffMs)

	for _, consumer := range c.consumers {
		consumer := consumer
		c.receiveWg.Add(1)
		go func() {
			defer c.receiveWg.Done()
			partitionReceiveLoop(ctxWithCancel, consumer, msgChan, retryManager, c.shutdown)
		}()
	}

	go func() {
		c.receiveWg.Wait()
		close(msgChan)
	}()

	return msgChan, nil
}

// Ack acknowledges a received message.
func (c *Consumer) Ack(ctx context.Context, message *proto.StreamMessage) (*proto.AckResponse, error) {
	topic_name := message.GetMsgId().GetTopicName()
	topic_consumer := c.consumers[topic_name]
	return topic_consumer.sendAck(ctx, message.GetRequestId(), message.GetMsgId(), c.subscription)
}

// Close stops receive loops and signals topic consumers to stop.
func (c *Consumer) Close() {
	c.shutdown.Store(true)
	if c.receiveCancel != nil {
		c.receiveCancel()
	}
	for _, consumer := range c.consumers {
		consumer.stop()
	}
	// allow broker to observe closure
	time.Sleep(100 * time.Millisecond)
}

func partitionReceiveLoop(
	ctx context.Context,
	consumer *topicConsumer,
	msgChan chan<- *proto.StreamMessage,
	retryManager retryManager,
	shutdown *atomic.Bool,
) {
	attempts := 0

	for {
		if shutdown.Load() {
			return
		}

		stream, err := consumer.receive(ctx)
		if err == nil {
			attempts = 0
			for !shutdown.Load() && !consumer.stopSignal.Load() {
				message, recvErr := stream.Recv()
				if recvErr != nil {
					err = recvErr
					break
				}
				select {
				case msgChan <- message:
				case <-ctx.Done():
					return
				}
			}
		}

		if shutdown.Load() {
			return
		}

		if consumer.stopSignal.Load() {
			consumer.stopSignal.Store(false)
			if err := resubscribe(ctx, consumer); err != nil {
				return
			}
			continue
		}

		if err != nil && isUnrecoverable(err) {
			if err := resubscribe(ctx, consumer); err != nil {
				return
			}
			attempts = 0
			continue
		}

		if err != nil && retryManager.isRetryable(err) {
			attempts++
			if attempts > retryManager.maxRetriesValue() {
				if err := resubscribe(ctx, consumer); err != nil {
					return
				}
				attempts = 0
				continue
			}
			backoff := retryManager.calculateBackoff(attempts - 1)
			time.Sleep(backoff)
			continue
		}

		if err != nil {
			return
		}
	}
}

func resubscribe(ctx context.Context, consumer *topicConsumer) error {
	_, err := consumer.subscribe(ctx)
	return err
}
