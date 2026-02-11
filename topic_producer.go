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

// Producer represents a message producer that is responsible for sending
// messages to a specific topic on a message broker. It handles producer
// creation, message sending, and maintains the producer's state.
type topicProducer struct {
	client            *DanubeClient
	topic             string                      // name of the topic to which the producer sends messages.
	producerName      string                      // name assigned to the producer instance.
	producerID        uint64                      // The unique identifier for the producer, provided by broker
	requestID         atomic.Uint64               // atomic counter for generating unique request IDs.
	schemaRef         *proto.SchemaReference      // optional schema registry reference
	schemaID          *uint64                     // resolved schema id
	schemaVersion     *uint32                     // resolved schema version
	dispatch_strategy *ConfigDispatchStrategy     // The way the messages will be delivered to consumers
	producerOptions   ProducerOptions             // Options that configure the behavior of the producer.
	streamClient      proto.ProducerServiceClient // gRPC client used for communication with the message broker.
	stopSignal        *atomic.Bool                // An atomic boolean signal used to indicate if the producer should stop.
	brokerAddr        string
	retryManager      retryManager
}

func newTopicProducer(
	client *DanubeClient,
	topic string,
	producerName string,
	schemaRef *proto.SchemaReference,
	dispatch_strategy *ConfigDispatchStrategy,
	producerOptions ProducerOptions,
) topicProducer {
	retryManager := newRetryManager(producerOptions.MaxRetries, producerOptions.BaseBackoffMs, producerOptions.MaxBackoffMs)

	return topicProducer{
		client:            client,
		topic:             topic,
		producerName:      producerName,
		producerID:        0,
		requestID:         atomic.Uint64{},
		schemaRef:         schemaRef,
		dispatch_strategy: dispatch_strategy,
		producerOptions:   producerOptions,
		streamClient:      nil,
		stopSignal:        &atomic.Bool{},
		brokerAddr:        client.URI,
		retryManager:      retryManager,
	}
}

// create initializes the producer and registers it with the message broker.
//
// This method connects to the broker, sets up the producer with the provided schema,
// and starts a health check service. It handles retries in case of failures and
// updates the producerID upon successful creation.
//
// Parameters:
// - ctx: The context for managing request lifecycle and cancellation.
//
// Returns:
// - uint64: The unique ID of the created producer if successful.
// - error: An error if producer creation fails.
func (p *topicProducer) create(ctx context.Context) (uint64, error) {
	attempts := 0

	for {
		producerID, err := p.tryCreate(ctx)
		if err == nil {
			return producerID, nil
		}

		if !p.retryManager.isRetryable(err) {
			return 0, err
		}

		attempts++
		if attempts > p.retryManager.maxRetriesValue() {
			return 0, err
		}

		p.lookupNewBroker(ctx)
		backoff := p.retryManager.calculateBackoff(attempts - 1)
		time.Sleep(backoff)
	}
}

func (p *topicProducer) tryCreate(ctx context.Context) (uint64, error) {
	if err := p.connect(p.brokerAddr); err != nil {
		return 0, err
	}

	if p.dispatch_strategy == nil {
		p.dispatch_strategy = NewConfigDispatchStrategy()
	}

	req := &proto.ProducerRequest{
		RequestId:          p.requestID.Add(1),
		ProducerName:       p.producerName,
		TopicName:          p.topic,
		SchemaRef:          p.schemaRef,
		ProducerAccessMode: proto.ProducerAccessMode_Shared,
		DispatchStrategy:   p.dispatch_strategy.toProtoDispatchStrategy(),
	}

	ctxWithAuth, err := p.client.authService.attachTokenIfNeeded(ctx, p.client.connectionManager.connectionOptions.APIKey, p.brokerAddr)
	if err != nil {
		return 0, err
	}

	resp, err := p.streamClient.CreateProducer(ctxWithAuth, req)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return 0, fmt.Errorf("producer already exists: %v", err)
		}
		return 0, err
	}

	p.producerID = resp.ProducerId

	if err := p.client.healthCheckService.StartHealthCheck(ctx, p.brokerAddr, 0, p.producerID, p.stopSignal); err != nil {
		return 0, err
	}

	if p.schemaRef != nil {
		schemaID, schemaVersion, err := p.resolveSchemaMetadata(ctx, p.schemaRef)
		if err != nil {
			return 0, err
		}
		p.schemaID = &schemaID
		p.schemaVersion = &schemaVersion
	}

	return p.producerID, nil
}

func (p *topicProducer) lookupNewBroker(ctx context.Context) {
	if newAddr, err := p.client.lookupService.handleLookup(ctx, p.brokerAddr, p.topic); err == nil {
		p.brokerAddr = newAddr
	}
}

// send sends a message to the topic associated with this producer.
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
func (p *topicProducer) send(ctx context.Context, data []byte, attributes map[string]string) (uint64, error) {
	if p.streamClient == nil {
		return 0, unrecoverableError("Send: producer is not connected")
	}

	if attributes == nil {
		attributes = make(map[string]string)
	}

	publishTime := uint64(time.Now().UnixMilli())

	msgID := &proto.MsgID{
		ProducerId: p.producerID,
		TopicName:  p.topic,
		BrokerAddr: p.brokerAddr,
	}

	req := &proto.StreamMessage{
		RequestId:        p.requestID.Add(1),
		MsgId:            msgID,
		Payload:          data,
		PublishTime:      publishTime,
		ProducerName:     p.producerName,
		SubscriptionName: "",
		Attributes:       attributes,
		SchemaId:         p.schemaID,
		SchemaVersion:    p.schemaVersion,
	}

	ctxWithAuth, err := p.client.authService.attachTokenIfNeeded(ctx, p.client.connectionManager.connectionOptions.APIKey, p.brokerAddr)
	if err != nil {
		return 0, err
	}

	res, err := p.streamClient.SendMessage(ctxWithAuth, req)
	if err != nil {
		return 0, err
	}

	return res.RequestId, nil
}

func (p *topicProducer) connect(addr string) error {
	conn, err := p.client.connectionManager.getConnection(addr, addr)
	if err != nil {
		return err
	}
	p.streamClient = proto.NewProducerServiceClient(conn.grpcConn)
	return nil
}

func (p *topicProducer) resolveSchemaMetadata(ctx context.Context, schemaRef *proto.SchemaReference) (uint64, uint32, error) {
	client := p.client.Schema()

	if schemaRef == nil {
		return 0, 0, fmt.Errorf("schema reference is nil")
	}

	switch ref := schemaRef.GetVersionRef().(type) {
	case *proto.SchemaReference_PinnedVersion:
		latest, err := client.GetLatestSchema(ctx, schemaRef.GetSubject())
		if err != nil {
			return 0, 0, err
		}
		if ref.PinnedVersion > latest.Version {
			return 0, 0, fmt.Errorf("pinned version %d does not exist for subject %s", ref.PinnedVersion, schemaRef.GetSubject())
		}
		if ref.PinnedVersion == latest.Version {
			return latest.SchemaID, latest.Version, nil
		}
		version := ref.PinnedVersion
		pinned, err := client.GetSchemaVersion(ctx, latest.SchemaID, &version)
		if err != nil {
			return 0, 0, err
		}
		return pinned.SchemaID, pinned.Version, nil
	case *proto.SchemaReference_MinVersion:
		latest, err := client.GetLatestSchema(ctx, schemaRef.GetSubject())
		if err != nil {
			return 0, 0, err
		}
		if latest.Version < ref.MinVersion {
			return 0, 0, fmt.Errorf("latest version %d does not meet minimum %d for subject %s", latest.Version, ref.MinVersion, schemaRef.GetSubject())
		}
		return latest.SchemaID, latest.Version, nil
	case *proto.SchemaReference_UseLatest:
		latest, err := client.GetLatestSchema(ctx, schemaRef.GetSubject())
		if err != nil {
			return 0, 0, err
		}
		return latest.SchemaID, latest.Version, nil
	default:
		latest, err := client.GetLatestSchema(ctx, schemaRef.GetSubject())
		if err != nil {
			return 0, 0, err
		}
		return latest.SchemaID, latest.Version, nil
	}
}
