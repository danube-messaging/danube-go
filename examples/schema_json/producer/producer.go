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

	client, err := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	topic := "/default/topic_json"
	jsonSchema := `{"type": "object", "properties": {"field1": {"type": "string"}, "field2": {"type": "integer"}}}`
	producerName := "producer_json"

	// Register schema in the schema registry (if not already registered)
	_, err = client.Schema().RegisterSchema("topic_json-value").
		WithType(danube.SchemaTypeJSONSchema).
		WithSchemaData([]byte(jsonSchema)).
		WithDescription("JSON schema for topic_json").
		Execute(ctx)
	if err != nil {
		log.Fatalf("failed to register schema: %v", err)
	}

	producer, err := client.NewProducer().
		WithName(producerName).
		WithTopic(topic).
		WithSchemaSubject("topic_json-value").
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
