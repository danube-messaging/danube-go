# Danube Test Cluster

Minimal single-broker setup for running integration tests locally.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+

## Start

```bash
cd docker/
docker compose up -d
```

## Verify

```bash
docker compose ps
# Both etcd and broker should show "Up" / "healthy"
```

## Run Integration Tests

```bash
# From the repository root
go test ./integration_tests/ -v -count=1
```

The tests connect to `http://127.0.0.1:6650` by default. Override with:

```bash
DANUBE_BROKER_URL=http://your-host:6650 go test ./integration_tests/ -v -count=1
```

## Stop

```bash
docker compose down -v   # -v removes volumes for a fresh start
```

## Architecture

- **ETCD** — metadata store on port 2379
- **Single broker** — gRPC on port 6650, admin on 50051
- No TLS, no auth, filesystem backend
- Broker advertises as `127.0.0.1:6650` so topic lookups resolve from the host

