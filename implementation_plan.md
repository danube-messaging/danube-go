# Danube Go Client Parity Plan

## Objective
Bring `danube-go` to feature parity with the latest Rust client (`danube-client/src`), with a focus on schema registry support, auth/token handling, retry/backoff, and modern broker lookup behavior aligned to updated `proto/` definitions.

## Current gaps (vs Rust)
- **Schema registry missing**: Go uses legacy schema types + `Discovery.GetSchema`, while the updated API uses `SchemaRegistry` (new proto). No `SchemaReference` usage.
- **Producer/consumer parity gaps**:
  - Producer still sends deprecated `Schema` and doesn’t attach `schema_id`/`schema_version` on messages.
  - Consumer receive loop lacks retry/backoff/resubscribe behavior and graceful shutdown logic.
  - Topic-level producers/consumers mutate shared `client.URI` instead of tracking broker address per topic.
- **Auth/token support missing**: No API key/JWT token flow or metadata injection.
- **Retry/backoff**: Ad hoc retries only; no shared retry manager with jitter and unified retryable error classification.
- **TLS/mTLS config**: Connection manager only supports insecure gRPC.
- **Schema types mismatch**: Go defines `Schema`, `SchemaType` (legacy), but should expose `SchemaType`, `CompatibilityMode`, `SchemaInfo`, and schema registry helpers.

## Phase 0 — Proto alignment & cleanup
**Goal:** ensure Go code uses the updated protos and remove old schema logic.
- Replace legacy schema fetch paths (`schema_service.go`) with Schema Registry client.
- Remove or deprecate `Schema` model (`schema.go`) and `SchemaRequest` usage.
- Verify `proto/SchemaRegistry.proto` is generated and used by Go code.
- Update README snippet for generating Go protos (include SchemaRegistry).

## Phase 1 — Core infrastructure parity
**Goal:** shared connection options, auth handling, and retry infrastructure.
1. **Connection options & TLS**
   - Introduce `ConnectionOptions` struct (TLS, mTLS, api_key) similar to Rust.
   - Update `rpc_connection.go` to use TLS credentials when configured.
2. **Auth service**
   - Add `auth_service.go` with:
     - `Authenticate(api_key)`
     - token cache + expiry
     - `InsertTokenIfNeeded(ctx, req)` using gRPC metadata
3. **Retry manager**
   - Add `retry_manager.go` with linear backoff + jitter and `isRetryableError`.
   - Centralize auth injection in retry manager helpers.

## Phase 2 — Schema Registry client
**Goal:** implement Schema Registry operations and Go-friendly types.
- Add `schema_registry_client.go`:
  - `RegisterSchema` builder (subject, type, definition, description, created_by, tags)
  - `GetSchemaByID`, `GetLatestSchema`, `GetSchemaVersion`
  - `ListVersions`, `CheckCompatibility`, `SetCompatibilityMode`
- Add `schema_types.go`:
  - `SchemaType` (bytes/string/number/avro/json_schema/protobuf)
  - `CompatibilityMode` (none/backward/forward/full)
  - `SchemaInfo` wrapper + `SchemaDefinitionAsString()` helper
- Add `DanubeClient.Schema()` accessor (mirrors Rust).

## Phase 3 — Producer parity
**Goal:** support schema registry references and retry behavior equivalent to Rust.
1. **API changes**
   - Update `ProducerBuilder` to accept schema registry references:
     - `WithSchemaSubject(subject)`
     - `WithSchemaVersion(subject, pinned)`
     - `WithSchemaMinVersion(subject, min)`
     - `WithSchemaReference(ref)`
   - Deprecate/remove `WithSchema(schemaName, schemaType, schemaData)`.
2. **TopicProducer refactor**
   - Store `brokerAddr` per topic producer; do **not** mutate `client.URI`.
   - On `Create`:
     - call `CreateProducer` with `SchemaReference` (if provided)
     - resolve schema metadata via registry; cache `schema_id/schema_version`
   - On `Send`:
     - attach cached schema metadata to `StreamMessage`
   - Use `RetryManager` for create/send with broker lookup + recreate after max retries.

## Phase 4 — Consumer parity
**Goal:** robust receive loop + graceful shutdown consistent with Rust.
- Topic consumer stores `brokerAddr`, uses retry manager for subscribe/receive.
- `Consumer.Receive` uses per-partition loops that:
  - open stream
  - forward messages to channel
  - retry on transient errors with backoff
  - resubscribe on broker CLOSE signal
- Add `Consumer.Close()` to stop receive loops and health checks gracefully.
- Insert auth tokens for Subscribe/Receive/Ack.

## Phase 5 — API polish, docs, and tests
- Update README examples (schema registry + api key + TLS/mTLS).
- Add examples:
  - producer with schema registry reference
  - schema registry usage (register/get/check compatibility)
- Add tests:
  - unit tests for schema registry client + retry manager
  - integration tests that create producer/consumer with schema validation

## File map (expected new/updated)
- **New**: `auth_service.go`, `retry_manager.go`, `schema_registry_client.go`, `schema_types.go`
- **Update**: `client.go`, `rpc_connection.go`, `connection_manager.go`, `producer.go`, `producer_builder.go`, `topic_producer.go`, `consumer.go`, `topic_consumer.go`, `health_check.go`, `README.md`
- **Remove/Deprecate**: `schema_service.go`, legacy `Schema` usage in `schema.go`

## Acceptance criteria
- Go client supports schema registry operations and schema references on producers.
- Producers send `schema_id`/`schema_version` with messages when configured.
- Auth token is automatically attached when API key is configured.
- Retry/backoff logic is centralized and used for create/send/receive.
- No shared `client.URI` mutation when broker redirects.
- Updated docs/examples compile against the new protos.
