package main

import (
	"context"
	"fmt"
	"log"
	"time"

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
	producerName := "producer_part"

	producer, err := client.NewProducer().
		WithName(producerName).
		WithTopic(topic).
		WithPartitions(3).
		Build()
	if err != nil {
		log.Fatalf("unable to initialize the producer: %v", err)
	}

	if err := producer.Create(ctx); err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	log.Printf("The Producer %s was created", producerName)

	for i := 0; i < 200; i++ {

		payload := fmt.Sprintf("Hello Danube %d", i)

		// Convert string to bytes
		bytes_payload := []byte(payload)

		messageID, err := producer.Send(ctx, bytes_payload, nil)
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}
		log.Printf("The Message with id %v was sent", messageID)

		time.Sleep(1 * time.Second)
	}
}
