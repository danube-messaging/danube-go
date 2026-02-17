package danube

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/danube-messaging/danube-go/proto"
)

// Producer represents a message producer that is responsible for sending
// messages to a specific partitioned or non-partitioned topic on a message broker.
// It handles producer creation, message sending, and maintains the producer's state.
type Producer struct {
	mu                sync.Mutex
	client            *DanubeClient
	topicName         string                  // Name of the topic to which the producer sends messages.
	schemaRef         *proto.SchemaReference  // Schema registry reference (optional)
	dispatch_strategy *ConfigDispatchStrategy // The way the messages will be delivered to consumers
	producerName      string                  // Name assigned to the producer instance.
	partitions        int32                   // The number of partitions for the topic
	messageRouter     *messageRouter          // The way the messages will be delivered to consumers
	producers         []*topicProducer        // All the underhood producers, for sending messages to topic partitions
	producerOptions   ProducerOptions         // Options that configure the behavior of the producer.
}

func newProducer(
	client *DanubeClient,
	topicName string,
	partitions int32,
	producerName string,
	schemaRef *proto.SchemaReference,
	dispatch_strategy *ConfigDispatchStrategy,
	producerOptions ProducerOptions,
) *Producer {
	if dispatch_strategy == nil {
		dispatch_strategy = NewConfigDispatchStrategy()
	}

	return &Producer{
		client:            client,
		topicName:         topicName,
		schemaRef:         schemaRef,
		dispatch_strategy: dispatch_strategy,
		producerName:      producerName,
		partitions:        partitions, // 0 for non-partitioned
		messageRouter:     nil,
		producerOptions:   producerOptions,
	}
}

// Create initializes the producer and registers it with the message brokers.
//
// Parameters:
// - ctx: The context for managing request lifecycle and cancellation.
//
// Returns:
// - error: An error if producer creation fails.
func (p *Producer) Create(ctx context.Context) error {

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.partitions == 0 {
		// Create a single TopicProducer for non-partitioned topic
		topicProducer := newTopicProducer(
			p.client,
			p.topicName,
			p.producerName,
			p.schemaRef,
			p.dispatch_strategy,
			p.producerOptions,
		)

		_, err := topicProducer.create(ctx)
		if err != nil {
			return err
		}

		p.producers = append(p.producers, &topicProducer)

	} else {
		if p.messageRouter == nil {
			p.messageRouter = newMessageRouter(p.partitions)
		}

		producers := make([]*topicProducer, p.partitions)
		errChan := make(chan error, p.partitions)
		doneChan := make(chan struct{}, p.partitions)

		for partitionID := int32(0); partitionID < p.partitions; partitionID++ {
			go func(partitionID int32) {
				defer func() { doneChan <- struct{}{} }()

				// Generate the topic string with partition ID
				topicName := fmt.Sprintf("%s-part-%d", p.topicName, partitionID)

				// Generate the producer name with partition ID
				producerName := fmt.Sprintf("%s-%d", p.producerName, partitionID)

				// Create a new TopicProducer instance
				topicProducer := newTopicProducer(
					p.client,
					topicName,
					producerName,
					p.schemaRef,
					p.dispatch_strategy,
					p.producerOptions,
				)

				// Create the topic producer
				_, err := topicProducer.create(ctx)
				if err != nil {
					errChan <- err
					return
				}

				producers[partitionID] = &topicProducer
			}(partitionID)
		}

		// Wait for all goroutines to finish and check for errors
		for i := int32(0); i < p.partitions; i++ {
			select {
			case err := <-errChan:
				// If any error occurs, return immediately
				return err
			case <-doneChan:
				// Process completion
			}
		}

		p.producers = producers
	}

	return nil
}

// Send sends a message to the topic associated with this producer.
//
// It constructs a message request and sends it to the broker. The method handles
// payload and error reporting. It assumes that the producer has been successfully
// created and is ready to send messages.
//
// Parameters:
// - ctx: The context for managing request lifecycle and cancellation.
// - data: The message payload to be sent.
// - attributes: user-defined properties or attributes associated with the message
//
// Returns:
// - uint64: The sequence ID of the sent message if successful.
// - error: An error if message sending fail
func (p *Producer) Send(ctx context.Context, data []byte, attributes map[string]string) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	partitionID := p.selectPartition()

	if partitionID >= int32(len(p.producers)) {
		return 0, fmt.Errorf("partition ID out of range")
	}

	retryManager := newRetryManager(p.producerOptions.MaxRetries, p.producerOptions.BaseBackoffMs, p.producerOptions.MaxBackoffMs)
	attempts := 0

	for {
		requestID, err := p.producers[partitionID].send(ctx, data, attributes)
		if err == nil {
			return requestID, nil
		}

		if isUnrecoverable(err) {
			if err := p.recreateProducer(ctx, partitionID); err != nil {
				return 0, err
			}
			attempts = 0
			continue
		}

		if retryManager.isRetryable(err) {
			attempts++
			if attempts > retryManager.maxRetriesValue() {
				if err := p.lookupAndRecreate(ctx, partitionID, err); err != nil {
					return 0, err
				}
				attempts = 0
				continue
			}
			backoff := retryManager.calculateBackoff(attempts - 1)
			time.Sleep(backoff)
			continue
		}

		return 0, err
	}
}

func (p *Producer) selectPartition() int32 {
	if p.partitions > 0 && p.messageRouter != nil {
		return p.messageRouter.roundRobin()
	}
	return 0
}

func (p *Producer) recreateProducer(ctx context.Context, partitionID int32) error {
	producer := p.producers[partitionID]
	_, err := producer.create(ctx)
	return err
}

func (p *Producer) lookupAndRecreate(ctx context.Context, partitionID int32, originalErr error) error {
	producer := p.producers[partitionID]
	addr, err := producer.client.lookupService.handleLookup(ctx, producer.connectURL, producer.topic)
	if err != nil {
		return originalErr
	}
	producer.brokerAddr = addr.BrokerURL
	producer.connectURL = addr.ConnectURL
	producer.proxy = addr.Proxy
	_, err = producer.create(ctx)
	return err
}
