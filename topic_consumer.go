package danube

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/danube-messaging/danube-go/proto"
)

// topicConsumer represents a message consumer that subscribes to a topic or topic partition and receives messages.
// It handles communication with the message broker and manages the consumer's state.
type topicConsumer struct {
	client           *DanubeClient
	topicName        string                      // the name of the topic that the consumer subscribes to
	consumerName     string                      // the name assigned to the consumer instance
	consumerID       uint64                      // the unique identifier of the consumer assigned by the broker after subscription
	subscription     string                      // the name of the subscription for the consumer
	subscriptionType SubType                     // the type of subscription (e.g., EXCLUSIVE, SHARED, FAILOVER)
	consumerOptions  ConsumerOptions             // configuration options for the consumer
	requestID        atomic.Uint64               // atomic counter for generating unique request IDs
	streamClient     proto.ConsumerServiceClient // the gRPC client used to communicate with the consumer service
	stopSignal       *atomic.Bool                // atomic boolean flag to indicate if the consumer should be stopped
}

func newTopicConsumer(
	client *DanubeClient,
	topicName, consumerName, subscription string,
	subType *SubType,
	options ConsumerOptions,
) topicConsumer {
	var subscriptionType SubType
	if subType != nil {
		subscriptionType = *subType
	} else {
		subscriptionType = Shared
	}

	return topicConsumer{
		client:           client,
		topicName:        topicName,
		consumerName:     consumerName,
		subscription:     subscription,
		subscriptionType: subscriptionType,
		consumerOptions:  options,
		stopSignal:       &atomic.Bool{},
	}
}

// subscribe initializes the subscription to the topic and starts the health check service.
// It establishes a gRPC connection with the broker and requests to subscribe to the topic.
//
// Parameters:
// - ctx: The context for managing the subscription lifecycle.
//
// Returns:
// - uint64: The unique identifier assigned to the consumer by the broker.
// - error: An error if the subscription fails or if initialization encounters issues.
func (c *topicConsumer) subscribe(ctx context.Context) (uint64, error) {
	brokerAddr, err := c.client.lookupService.handleLookup(ctx, c.client.URI, c.topicName)
	if err != nil {
		return 0, err
	}

	// Initialize the gRPC client connection
	if err := c.connect(brokerAddr); err != nil {
		return 0, err
	}

	req := &proto.ConsumerRequest{
		RequestId:        c.requestID.Add(1),
		TopicName:        c.topicName,
		ConsumerName:     c.consumerName,
		Subscription:     c.subscription,
		SubscriptionType: proto.ConsumerRequest_SubscriptionType(c.subscriptionType),
	}

	resp, err := c.streamClient.Subscribe(ctx, req)
	if err != nil {
		return 0, err
	}

	c.consumerID = resp.GetConsumerId()

	// Start health check service
	err = c.client.healthCheckService.StartHealthCheck(ctx, brokerAddr, 1, c.consumerID, c.stopSignal)
	if err != nil {
		return 0, err
	}

	return c.consumerID, nil
}

// receive starts receiving messages from the subscribed topic or topic partition.
// It continuously polls for new messages and handles them as long as the stopSignal has not been set to true.
//
// Parameters:
// - ctx: The context for managing the receive operation.
//
// Returns:
// - proto.ConsumerService_ReceiveMessagesClient: A client for receiving messages from the broker.
// - error: An error if the receive client cannot be created or if other issues occur.
func (c *topicConsumer) receive(ctx context.Context) (proto.ConsumerService_ReceiveMessagesClient, error) {
	if c.streamClient == nil {
		return nil, errors.New("stream client not initialized")
	}

	req := &proto.ReceiveRequest{
		RequestId:  c.requestID.Add(1),
		ConsumerId: c.consumerID,
	}

	// Check if stopSignal is set
	if c.stopSignal.Load() {
		log.Println("Consumer has been stopped by broker, attempting to resubscribe.")

		maxRetries := 3
		attempts := 0
		var err error

		for attempts < maxRetries {
			if _, err = c.subscribe(ctx); err == nil {
				log.Println("Successfully resubscribed.")
				return c.streamClient.ReceiveMessages(ctx, req)
			}

			attempts++
			log.Printf("Resubscription attempt %d/%d failed: %v", attempts, maxRetries, err)
			time.Sleep(2 * time.Second) // Wait before retrying
		}

		return nil, fmt.Errorf("failed to resubscribe after %d attempts: %v", maxRetries, err)
	}

	return c.streamClient.ReceiveMessages(ctx, req)
}

// sendAck sends an acknowledgement for a message to the broker.
func (c *topicConsumer) sendAck(ctx context.Context, req_id uint64, msg_id *proto.MsgID, subscription_name string) (*proto.AckResponse, error) {
	if c.streamClient == nil {
		return nil, errors.New("stream client not initialized")
	}

	ack_req := &proto.AckRequest{
		RequestId:        req_id,
		MsgId:            msg_id,
		SubscriptionName: subscription_name,
	}

	return c.streamClient.Ack(ctx, ack_req)
}

func (c *topicConsumer) connect(addr string) error {
	conn, err := c.client.connectionManager.getConnection(addr, addr)
	if err != nil {
		return err
	}
	c.streamClient = proto.NewConsumerServiceClient(conn.grpcConn)
	return nil
}
