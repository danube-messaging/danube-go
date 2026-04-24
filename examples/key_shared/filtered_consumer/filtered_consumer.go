package main

import (
	"context"
	"fmt"
	"log"

	"github.com/danube-messaging/danube-go"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	client, err := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	topic := "/default/topic_key_shared"
	consumerName := "consumer_payments_only"
	subscriptionName := "sub_key_shared"
	subType := danube.KeyShared

	// This consumer only receives messages whose routing key matches
	// "payment" or "invoice". All other keys are routed to other consumers.
	consumer, err := client.NewConsumer().
		WithConsumerName(consumerName).
		WithTopic(topic).
		WithSubscription(subscriptionName).
		WithSubscriptionType(subType).
		WithKeyFilter("payment").
		WithKeyFilter("invoice").
		Build()
	if err != nil {
		log.Fatalf("Failed to initialize the consumer: %v", err)
	}

	if err := consumer.Subscribe(ctx); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	log.Printf("The Consumer %s was created (filters: payment, invoice)", consumerName)

	stream, err := consumer.Receive(ctx)
	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}

	for msg := range stream {
		routingKey := msg.GetRoutingKey()
		fmt.Printf("Received: key=%s payload=%s\n",
			routingKey,
			string(msg.GetPayload()),
		)

		if _, err := consumer.Ack(ctx, msg); err != nil {
			log.Fatalf("Failed to acknowledge message: %v", err)
		}
	}
}
