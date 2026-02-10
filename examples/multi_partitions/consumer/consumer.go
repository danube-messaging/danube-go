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

	client, err := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	topic := "/default/partitioned_topic"
	consumerName := "consumer_part"
	subscriptionName := "subscription_part"
	subType := danube.Exclusive

	consumer, err := client.NewConsumer().
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

		fmt.Printf("Received message: %+v\n", string(msg.GetPayload()))
		if _, err := consumer.Ack(ctx, msg); err != nil {
			log.Fatalf("Failed to acknowledge message: %v", err)
		}

	}
}
