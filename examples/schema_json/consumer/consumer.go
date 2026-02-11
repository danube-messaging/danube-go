package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/danube-messaging/danube-go"
)

type MyMessage struct {
	Field1 string `json:"field1"`
	Field2 int    `json:"field2"`
}

func main() {
	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	client, err := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	topic := "/default/topic_json"
	consumerName := "consumer_json"
	subscriptionName := "subscription_json"
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

		var myMessage MyMessage
		if err := json.Unmarshal(msg.GetPayload(), &myMessage); err != nil {
			log.Printf("Failed to decode message: %v", err)
		} else {
			fmt.Printf("Received message: %+v\n", myMessage)
			if _, err := consumer.Ack(ctx, msg); err != nil {
				log.Fatalf("Failed to acknowledge message: %v", err)
			}
		}
	}
}
