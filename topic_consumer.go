package danube

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/danube-messaging/danube-go/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	brokerAddr       string
	retryManager     RetryManager
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

	retryManager := NewRetryManager(options.MaxRetries, options.BaseBackoffMs, options.MaxBackoffMs)

	return topicConsumer{
		client:           client,
		topicName:        topicName,
		consumerName:     consumerName,
		subscription:     subscription,
		subscriptionType: subscriptionType,
		consumerOptions:  options,
		stopSignal:       &atomic.Bool{},
		brokerAddr:       client.URI,
		retryManager:     retryManager,
	}
}

// stop signals this consumer to stop background activities.
func (c *topicConsumer) stop() {
	c.stopSignal.Store(true)
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
	attempts := 0

	for {
		consumerID, err := c.trySubscribe(ctx)
		if err == nil {
			return consumerID, nil
		}

		if !c.retryManager.IsRetryable(err) {
			return 0, err
		}

		attempts++
		if attempts > c.retryManager.MaxRetries() {
			return 0, err
		}

		c.lookupNewBroker(ctx)
		backoff := c.retryManager.CalculateBackoff(attempts - 1)
		time.Sleep(backoff)
	}
}

func (c *topicConsumer) trySubscribe(ctx context.Context) (uint64, error) {
	if err := c.connect(c.brokerAddr); err != nil {
		return 0, err
	}

	req := &proto.ConsumerRequest{
		RequestId:        c.requestID.Add(1),
		TopicName:        c.topicName,
		ConsumerName:     c.consumerName,
		Subscription:     c.subscription,
		SubscriptionType: proto.ConsumerRequest_SubscriptionType(c.subscriptionType),
	}

	ctxWithAuth, err := c.client.authService.attachTokenIfNeeded(ctx, c.client.connectionManager.connectionOptions.APIKey, c.brokerAddr)
	if err != nil {
		return 0, err
	}

	resp, err := c.streamClient.Subscribe(ctxWithAuth, req)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return 0, fmt.Errorf("consumer already exists: %v", err)
		}
		return 0, err
	}

	c.consumerID = resp.GetConsumerId()

	if err := c.client.healthCheckService.StartHealthCheck(ctx, c.brokerAddr, 1, c.consumerID, c.stopSignal); err != nil {
		return 0, err
	}

	return c.consumerID, nil
}

func (c *topicConsumer) lookupNewBroker(ctx context.Context) {
	if newAddr, err := c.client.lookupService.handleLookup(ctx, c.brokerAddr, c.topicName); err == nil {
		c.brokerAddr = newAddr
	}
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
		return nil, UnrecoverableError("Receive: consumer is not connected")
	}

	req := &proto.ReceiveRequest{
		RequestId:  c.requestID.Add(1),
		ConsumerId: c.consumerID,
	}

	ctxWithAuth, err := c.client.authService.attachTokenIfNeeded(ctx, c.client.connectionManager.connectionOptions.APIKey, c.brokerAddr)
	if err != nil {
		return nil, err
	}

	return c.streamClient.ReceiveMessages(ctxWithAuth, req)
}

// sendAck sends an acknowledgement for a message to the broker.
func (c *topicConsumer) sendAck(ctx context.Context, req_id uint64, msg_id *proto.MsgID, subscription_name string) (*proto.AckResponse, error) {
	if c.streamClient == nil {
		return nil, UnrecoverableError("SendAck: consumer is not connected")
	}

	ackReq := &proto.AckRequest{
		RequestId:        req_id,
		MsgId:            msg_id,
		SubscriptionName: subscription_name,
	}

	ctxWithAuth, err := c.client.authService.attachTokenIfNeeded(ctx, c.client.connectionManager.connectionOptions.APIKey, c.brokerAddr)
	if err != nil {
		return nil, err
	}

	return c.streamClient.Ack(ctxWithAuth, ackReq)
}

func (c *topicConsumer) connect(addr string) error {
	conn, err := c.client.connectionManager.getConnection(addr, addr)
	if err != nil {
		return err
	}
	c.brokerAddr = addr
	c.streamClient = proto.NewConsumerServiceClient(conn.grpcConn)
	return nil
}
