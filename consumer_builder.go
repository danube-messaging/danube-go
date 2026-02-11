package danube

import "errors"

// ConsumerBuilder is a builder for creating a new Consumer instance. It allows
// setting various properties for the consumer such as topic, name, subscription,
// subscription type, and options.
type ConsumerBuilder struct {
	client           *DanubeClient
	topic            string
	consumerName     string
	subscription     string
	subscriptionType *SubType
	consumerOptions  ConsumerOptions
}

func newConsumerBuilder(client *DanubeClient) *ConsumerBuilder {
	return &ConsumerBuilder{client: client}
}

// WithTopic sets the topic name for the consumer. This is a required field.
//
// Parameters:
// - topic: The name of the topic for the consumer.
func (b *ConsumerBuilder) WithTopic(topic string) *ConsumerBuilder {
	b.topic = topic
	return b
}

// WithConsumerName sets the name of the consumer. This is a required field.
//
// Parameters:
// - name: The name assigned to the consumer instance.
func (b *ConsumerBuilder) WithConsumerName(name string) *ConsumerBuilder {
	b.consumerName = name
	return b
}

// WithSubscription sets the name of the subscription for the consumer. This is a required field.
//
// Parameters:
// - subscription: The name of the subscription for the consumer.
func (b *ConsumerBuilder) WithSubscription(subscription string) *ConsumerBuilder {
	b.subscription = subscription
	return b
}

// WithSubscriptionType sets the type of subscription for the consumer. This field is optional.
//
// Parameters:
// - subType: The type of subscription (e.g., EXCLUSIVE, SHARED, FAILOVER).
func (b *ConsumerBuilder) WithSubscriptionType(subType SubType) *ConsumerBuilder {
	b.subscriptionType = &subType
	return b
}

// Build creates a new Consumer instance using the settings configured in the ConsumerBuilder.
// It performs validation to ensure that all required fields are set before creating the consumer.
//
// Returns:
// - *Consumer: A pointer to the newly created Consumer instance if successful.
// - error: An error if required fields are missing or if consumer creation fails.
func (b *ConsumerBuilder) Build() (*Consumer, error) {
	if b.topic == "" || b.consumerName == "" || b.subscription == "" {
		return nil, errors.New("missing required fields")
	}
	return newConsumer(b.client, b.topic, b.consumerName, b.subscription, b.subscriptionType, b.consumerOptions), nil
}

type ConsumerOptions struct {
	MaxRetries    int
	BaseBackoffMs int64
	MaxBackoffMs  int64
}
