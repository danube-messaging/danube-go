package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/danube-messaging/danube-go"
)

func main() {
	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	client := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()

	ctx := context.Background()
	topic := "/default/topic_json"
	jsonSchema := `{"type": "object", "properties": {"field1": {"type": "string"}, "field2": {"type": "integer"}}}`
	producerName := "producer_json"

	producer, err := client.NewProducer(ctx).
		WithName(producerName).
		WithTopic(topic).
		WithSchema("test_schema", danube.SchemaType_JSON, jsonSchema).
		Build()
	if err != nil {
		log.Fatalf("unable to initialize the producer: %v", err)
	}

	if err := producer.Create(ctx); err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	log.Printf("The Producer %s was created.", producerName)

	for i := 0; i < 200; i++ {
		data := map[string]interface{}{
			"field1": fmt.Sprintf("value%d", i),
			"field2": 2020 + i,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Fatalf("Failed to marshal data: %v", err)
		}

		messageID, err := producer.Send(ctx, jsonData, nil)
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}
		log.Printf("The Message with id %v was sent", messageID)

		time.Sleep(1 * time.Second)
	}
}
