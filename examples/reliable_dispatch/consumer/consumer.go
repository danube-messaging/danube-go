package main

import (
	"context"
	"fmt"
	"log"

	"github.com/danube-messaging/danube-go"
)

func main() {
	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	client := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()

	ctx := context.Background()
	topic := "/default/topic_reliable"
	consumerName := "consumer_reliable"
	subscriptionName := "subscription_reliable"
	subType := danube.Exclusive

	consumer, err := client.NewConsumer(ctx).
		WithConsumerName(consumerName).
		WithTopic(topic).
		WithSubscription(subscriptionName).
		WithSubscriptionType(subType).
		Build()
	if err != nil {
		log.Fatalf("Failed to initialize the consumer: %v", err)
	}

	if err := consumer.Subscribe(ctx); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	log.Printf("The Consumer %s was created", consumerName)

	// Receiving messages
	stream, err := consumer.Receive(ctx)
	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}

	for msg := range stream {
		fmt.Printf("Received message: %s, topic: %s, offset: %d\n", string(msg.GetPayload()), msg.GetMsgId().GetTopicName(), msg.GetMsgId().GetTopicOffset())

		if _, err := consumer.Ack(ctx, msg); err != nil {
			log.Fatalf("Failed to acknowledge message: %v", err)
		}

	}
}
