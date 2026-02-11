package danube

import (
	"fmt"

	"github.com/danube-messaging/danube-go/proto"
)

// ProducerBuilder is a builder for creating a new Producer instance. It allows
// setting various properties for the producer such as topic, name, schema, and options.
type ProducerBuilder struct {
	client            *DanubeClient
	topic             string
	producerName      string
	partitions        int32
	schemaRef         *proto.SchemaReference
	dispatch_strategy *ConfigDispatchStrategy
	producerOptions   ProducerOptions
}

func newProducerBuilder(client *DanubeClient) *ProducerBuilder {
	return &ProducerBuilder{
		client:          client,
		topic:           "",
		producerName:    "",
		schemaRef:       nil,
		producerOptions: ProducerOptions{},
	}
}

// WithTopic sets the topic name for the producer. This is a required field.
//
// Parameters:
// - topic: The name of the topic for the producer.
func (pb *ProducerBuilder) WithTopic(topic string) *ProducerBuilder {
	pb.topic = topic
	return pb
}

// WithName sets the name of the producer. This is a required field.
//
// Parameters:
// - producerName: The name assigned to the producer instance.
func (pb *ProducerBuilder) WithName(producerName string) *ProducerBuilder {
	pb.producerName = producerName
	return pb
}

// WithSchemaSubject sets the schema registry subject (uses latest version).
func (pb *ProducerBuilder) WithSchemaSubject(subject string) *ProducerBuilder {
	pb.schemaRef = &proto.SchemaReference{
		Subject: subject,
		VersionRef: &proto.SchemaReference_UseLatest{
			UseLatest: true,
		},
	}
	return pb
}

// WithSchemaVersion pins a schema subject to a specific version.
func (pb *ProducerBuilder) WithSchemaVersion(subject string, version uint32) *ProducerBuilder {
	pb.schemaRef = &proto.SchemaReference{
		Subject: subject,
		VersionRef: &proto.SchemaReference_PinnedVersion{
			PinnedVersion: version,
		},
	}
	return pb
}

// WithSchemaMinVersion enforces a minimum schema version.
func (pb *ProducerBuilder) WithSchemaMinVersion(subject string, minVersion uint32) *ProducerBuilder {
	pb.schemaRef = &proto.SchemaReference{
		Subject: subject,
		VersionRef: &proto.SchemaReference_MinVersion{
			MinVersion: minVersion,
		},
	}
	return pb
}

// WithSchemaReference sets a custom SchemaReference (advanced use).
func (pb *ProducerBuilder) WithSchemaReference(schemaRef *proto.SchemaReference) *ProducerBuilder {
	pb.schemaRef = schemaRef
	return pb
}

// WithDispatchStrategy sets the dispatch strategy for the producer.
// This method configures the retention strategy for the producer, which determines how messages are stored and managed.
//
// Parameters:
// - dispatch_strategy: The dispatch strategy for the producer.
func (pb *ProducerBuilder) WithDispatchStrategy(dispatch_strategy *ConfigDispatchStrategy) *ProducerBuilder {
	pb.dispatch_strategy = dispatch_strategy
	return pb
}

// WithPartitions sets the number of topic partitions.
//
// Parameters:
// - partitions: The number of partitions for a new topic.
func (pb *ProducerBuilder) WithPartitions(partitions int32) *ProducerBuilder {
	pb.partitions = partitions
	return pb
}

// WithOptions sets the configuration options for the producer. This allows for customization
// of producer behavior.
//
// Parameters:
// - options: Configuration options for the producer.
func (pb *ProducerBuilder) WithOptions(options ProducerOptions) *ProducerBuilder {
	pb.producerOptions = options
	return pb
}

// Build creates a new Producer instance using the settings configured in the ProducerBuilder.
// It performs validation to ensure that required fields are set before creating the producer.
//
// Returns:
// - *Producer: A pointer to the newly created Producer instance if successful.
// - error: An error if required fields are missing or if producer creation fails.
func (pb *ProducerBuilder) Build() (*Producer, error) {
	if pb.topic == "" {
		return nil, fmt.Errorf("topic must be set")
	}
	if pb.producerName == "" {
		return nil, fmt.Errorf("producer name must be set")
	}

	return newProducer(
		pb.client,
		pb.topic,
		pb.partitions,
		pb.producerName,
		pb.schemaRef,
		pb.dispatch_strategy,
		pb.producerOptions,
	), nil
}

// ProducerOptions configures retry behavior for producers.
type ProducerOptions struct {
	MaxRetries    int   // Maximum retry attempts for create/send operations.
	BaseBackoffMs int64 // Base backoff in milliseconds.
	MaxBackoffMs  int64 // Maximum backoff in milliseconds.
}
