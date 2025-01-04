# Danube-go client

The Go Client library for interacting with Danube Pub/Sub messaging platform.

[Danube](https://github.com/danrusei/danube) is an open-source **distributed** Pub/Sub messaging platform written in Rust. Consult [the documentation](https://dev-state.com/danube_docs/) for supported concepts and the platform architecture.

## Example usage

Check out the [example files](https://github.com/danrusei/danube-go/tree/main/examples).

### Start the Danube server

Use the [instructions from the documentation](https://dev-state.com/danube_docs/) to run the Danube broker/cluster.

### Create Producer

```go
client := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()

ctx := context.Background()
topic := "/default/test_topic"
producerName := "test_producer"

producer, err := client.NewProducer(ctx).
    WithName(producerName).
    WithTopic(topic).
    Build()
if err != nil {
    log.Fatalf("unable to initialize the producer: %v", err)
}

if err := producer.Create(ctx); err != nil {
    log.Fatalf("Failed to create producer: %v", err)
}
log.Printf("The Producer %s was created", producerName)

payload := fmt.Sprintln("Hello Danube")

// Convert string to bytes
bytes_payload := []byte(payload)

// You can send the payload along with the user defined attributes, in this case is nil
messageID, err := producer.Send(ctx, bytes_payload, nil)
if err != nil {
    log.Fatalf("Failed to send message: %v", err)
}
log.Printf("The Message with id %v was sent", messageID)
```

### Create Consumer

```go
client := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()

ctx := context.Background()
topic := "/default/test_topic"
consumerName := "test_consumer"
subscriptionName := "test_subscription"
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

// Request to subscribe to the topic and create the resources on the Danube Broker
if err := consumer.Subscribe(ctx); err != nil {
    log.Fatalf("Failed to subscribe: %v", err)
}
log.Printf("The Consumer %s was created", consumerName)

// Request to receive messages
stream, err := consumer.Receive(ctx)
if err != nil {
    log.Fatalf("Failed to receive messages: %v", err)
}

// consume the messages from the go channel
for msg := range stream {
    fmt.Printf("Received message: %+v\n", string(msg.GetPayload()))

    // Acknowledge the message
    if _, err := consumer.Ack(ctx, msg); err != nil {
        log.Fatalf("Failed to acknowledge message: %v", err)
    }
}
```

## Contribution

Working on improving and adding new features. Please feel free to contribute or report any issues you encounter.

### Use latest DanubeApi.proto file

Make sure the proto/DanubeApi.proto is the latest from [Danube project](https://github.com/danrusei/danube/tree/main/proto).

If not replace the file and add at the top of the file

```bash
option go_package = "github.com/danrusei/danube-go/proto";
```

right after the `package danube;`

In order to generate the Go grpc code you need the following packages installed:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

And generate the Go code from the proto file:

```bash
protoc --proto_path=./proto --go_out=./proto --go-grpc_out=./proto --go_opt=paths=source_relative      --go-grpc_opt=paths=source_relative proto/DanubeApi.proto
```
