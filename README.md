# Danube-go client

The Go client library for interacting with [Danube](https://github.com/danube-messaging/danube), an open-source distributed messaging platform written in Rust.

Consult [the documentation](https://danube-docs.dev-state.com/) for supported concepts and platform architecture.

## Install

```bash
go get github.com/danube-messaging/danube-go
```

## Producer

Producers publish messages to topics. A producer connects to a broker, creates a topic (or attaches to an existing one), and sends messages.

```go
producer, err := client.NewProducer().
    WithName("my-producer").
    WithTopic("/default/my-topic").
    Build()

producer.Create(ctx)
producer.Send(ctx, payload, attributes)
```

**Reliable dispatch** — messages are persisted to WAL and streamed to consumers with at-least-once delivery:

```go
producer, err := client.NewProducer().
    WithName("my-producer").
    WithTopic("/default/my-topic").
    WithDispatchStrategy(danube.NewReliableDispatchStrategy()).
    Build()
```

**Key-Shared routing** — tag messages with a routing key so all messages with the same key go to the same consumer:

```go
producer.SendWithKey(ctx, payload, nil, "order-123")
```

**Partitioned topics** — distribute load across multiple brokers:

```go
producer, err := client.NewProducer().
    WithTopic("/default/my-topic").
    WithName("my-producer").
    WithPartitions(3).
    Build()
```

**Schema registry** — attach a schema subject for producer-side validation:

```go
producer, err := client.NewProducer().
    WithTopic("/default/my-topic").
    WithName("my-producer").
    WithSchemaSubject("user-events-value").
    Build()
```

## Consumer

Consumers subscribe to topics and receive messages. Four subscription types control how messages are distributed:

| Type | Behavior |
|------|----------|
| `Exclusive` | Single consumer, total ordering |
| `Shared` | Round-robin across consumers |
| `FailOver` | Active-standby failover |
| `KeyShared` | Per-key ordering with multi-consumer parallelism |

```go
consumer, err := client.NewConsumer().
    WithTopic("/default/my-topic").
    WithConsumerName("my-consumer").
    WithSubscription("my-sub").
    WithSubscriptionType(danube.KeyShared).
    Build()

consumer.Subscribe(ctx)
stream, _ := consumer.Receive(ctx)

for msg := range stream {
    fmt.Println(string(msg.GetPayload()))
    consumer.Ack(ctx, msg)
}
```

**Key filtering** — in Key-Shared mode, receive only messages matching specific routing keys:

```go
consumer, err := client.NewConsumer().
    WithTopic("/default/my-topic").
    WithConsumerName("payments-worker").
    WithSubscription("orders-sub").
    WithSubscriptionType(danube.KeyShared).
    WithKeyFilter("payment").
    WithKeyFilter("invoice").
    Build()
```

**Negative acknowledgement** — reject a message with an optional delay for redelivery:

```go
consumer.Nack(ctx, msg, &delayMs, &reason)
```

## Examples

Full working examples are available in the [`examples/`](https://github.com/danube-messaging/danube-go/tree/main/examples) directory:

- **[schema_string](examples/schema_string)** — basic producer/consumer
- **[schema_json](examples/schema_json)** — JSON schema with registry
- **[reliable_dispatch](examples/reliable_dispatch)** — at-least-once delivery with acks
- **[multi_partitions](examples/multi_partitions)** — partitioned topic
- **[key_shared](examples/key_shared)** — Key-Shared routing, filtering, and producer with routing keys

## Running the Broker

Use the [instructions from the documentation](https://danube-docs.dev-state.com/) to run a Danube broker or cluster.

Quick start with Docker:

```bash
cd docker/
docker compose up -d
```

## Contribution

Working on improving and adding new features. Please feel free to contribute or report any issues you encounter.

### Running Integration Tests

```bash
# Start the test broker
cd docker/ && docker compose up -d

# Wait for healthy status
docker compose ps

# Run tests from the repository root
cd .. && go test ./integration_tests/ -v -count=1

# Clean up
cd docker/ && docker compose down -v
```

### Updating Proto Definitions

Ensure `proto/DanubeApi.proto` matches the latest from [danube-core](https://github.com/danube-messaging/danube/tree/main/danube-core/proto). Add the Go package option after `package danube;`:

```protobuf
option go_package = "github.com/danube-messaging/danube-go/proto";
```

Generate Go code:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

protoc --proto_path=./proto --go_out=./proto --go-grpc_out=./proto \
  --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative \
  proto/DanubeApi.proto

protoc --proto_path=./proto --go_out=./proto --go-grpc_out=./proto \
  --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative \
  proto/SchemaRegistry.proto
```
