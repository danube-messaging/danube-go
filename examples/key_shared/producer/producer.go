package main

import (
	"context"
	"fmt"
	"log"
	"time"

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
	producerName := "producer_key_shared"

	reliableStrategy := danube.NewReliableDispatchStrategy()

	producer, err := client.NewProducer().
		WithName(producerName).
		WithTopic(topic).
		WithDispatchStrategy(reliableStrategy).
		Build()
	if err != nil {
		log.Fatalf("unable to initialize the producer: %v", err)
	}

	if err := producer.Create(ctx); err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	log.Printf("The Producer %s was created", producerName)

	// Send messages with different routing keys.
	// All messages with the same key are guaranteed to be delivered
	// to the same consumer, in order.
	keys := []string{"payment", "shipping", "invoice", "payment", "shipping"}

	for i, key := range keys {
		payload := fmt.Sprintf("Order event #%d for key=%s", i, key)

		messageID, err := producer.SendWithKey(ctx, []byte(payload), nil, key)
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}
		log.Printf("Sent message id=%v key=%s payload=%s", messageID, key, payload)

		time.Sleep(500 * time.Millisecond)
	}
}
