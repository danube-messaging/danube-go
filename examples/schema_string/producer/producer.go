package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/danrusei/danube-go"
)

func main() {
	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	client := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()

	ctx := context.Background()
	topic := "/default/test_topic"

	producer, err := client.NewProducer(ctx).
		WithName("test_producer").
		WithTopic(topic).
		Build()
	if err != nil {
		log.Fatalf("unable to initialize the producer: %v", err)
	}

	producerID, err := producer.Create(ctx)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	log.Printf("The Producer was created with ID: %v", producerID)

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