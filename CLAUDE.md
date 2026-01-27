# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build all modules
mvn clean package

# Build specific module
mvn -pl app -am clean package
mvn -pl transformer -am clean package

# Skip tests
mvn clean package -DskipTests

# Run integration tests (requires Docker)
mvn -pl integration-tests verify
```

## Running the Application

```bash
# Build Docker images for app and transformer
./build-images.sh

# Start all infrastructure
docker-compose up -d

# Register Debezium connector (after services are healthy)
curl -X POST -H "Content-Type: application/json" \
  --data @docker/connect/register-connector.json \
  http://localhost:8083/connectors

# Check connector status
curl http://localhost:8083/connectors/event-connector/status
```

## Testing the Pipeline

```bash
# Create an event
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{"name": "test-event"}'

# Consume from output topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic event-details \
  --from-beginning
```

## Architecture

This is a CDC (Change Data Capture) pipeline using Debezium and Kafka Streams:

```
[PostgreSQL] --> [Debezium CDC] --> [Kafka: dbserver1.public.event]
                                           |
                                           v
                                    [Transformer App]
                                           | (REST call to app)
                                           v
                                    [Kafka: event-details]
```

**Modules:**
- `app`: Spring Boot REST API with Event entity (port 8080). Manages Event CRUD operations and serves as the source of truth. Uses Liquibase for schema migrations.
- `transformer`: Kafka Streams application (port 8081). Consumes CDC events from Debezium topic, fetches full event details via REST, and produces enriched events to output topic.
- `integration-tests`: End-to-end tests using Testcontainers. Spins up entire infrastructure and verifies the CDC pipeline.

**Key Topics:**
- `dbserver1.public.event`: Debezium CDC events (input)
- `event-details`: Enriched event data (output)

**Tech Stack:** Java 21, Spring Boot 3.4.x, PostgreSQL 16, Kafka/Zookeeper (Confluent 7.5), Debezium 2.4

## Transformer Runtime Configuration

The transformer Kafka Streams application can be tuned via environment variables at runtime.

### Parallelism & Threading

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_KAFKA_STREAMS_NUM_STREAM_THREADS` | 1 | Stream threads per instance. Set to input topic partition count for max parallelism. |

### Batching & Latency

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_KAFKA_STREAMS_COMMIT_INTERVAL_MS` | 1000 | Offset commit frequency. Lower = less duplicates on failure, higher = better throughput. |
| `SPRING_KAFKA_STREAMS_CACHE_MAX_BYTES_BUFFERING` | 10485760 (10MB) | Record cache size. Set to 0 for real-time with no caching. |
| `SPRING_KAFKA_STREAMS_POLL_MS` | 100 | Max poll blocking time. Lower = more responsive, higher CPU. |

### Consumer Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_KAFKA_STREAMS_MAX_POLL_RECORDS` | 1000 | Max records per poll. |
| `SPRING_KAFKA_STREAMS_FETCH_MAX_BYTES` | 52428800 (50MB) | Max data per fetch across all partitions. |
| `SPRING_KAFKA_STREAMS_FETCH_MAX_WAIT_MS` | 500 | Max broker wait time before responding. |

### Producer Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_KAFKA_STREAMS_BATCH_SIZE` | 16384 (16KB) | Producer batch size. Higher = better throughput, more latency. |
| `SPRING_KAFKA_STREAMS_LINGER_MS` | 100 | Wait time for batch to fill before sending. |
| `SPRING_KAFKA_STREAMS_BUFFER_MEMORY` | 33554432 (32MB) | Total producer buffer memory. |
| `SPRING_KAFKA_STREAMS_COMPRESSION_TYPE` | none | Compression: `none`, `gzip`, `snappy`, `lz4`, `zstd`. |

### Reliability

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_KAFKA_STREAMS_PROCESSING_GUARANTEE` | at_least_once | `at_least_once`, `exactly_once`, `exactly_once_v2` (Kafka 2.5+). |
| `SPRING_KAFKA_STREAMS_REPLICATION_FACTOR` | 1 | Replication for internal topics. Use 3 in production. |

### Scaling

To scale horizontally, run multiple transformer instances with the same `application-id`. Kafka Streams distributes partitions automatically. Max parallelism = number of input topic partitions.

**Example docker-compose override:**

```yaml
transformer:
  environment:
    SPRING_KAFKA_STREAMS_NUM_STREAM_THREADS: 4
    SPRING_KAFKA_STREAMS_COMMIT_INTERVAL_MS: 500
    SPRING_KAFKA_STREAMS_PROCESSING_GUARANTEE: exactly_once_v2
    SPRING_KAFKA_STREAMS_COMPRESSION_TYPE: lz4
```

**High-throughput profile:**
```yaml
SPRING_KAFKA_STREAMS_NUM_STREAM_THREADS: 4
SPRING_KAFKA_STREAMS_COMMIT_INTERVAL_MS: 5000
SPRING_KAFKA_STREAMS_BATCH_SIZE: 65536
SPRING_KAFKA_STREAMS_LINGER_MS: 200
SPRING_KAFKA_STREAMS_COMPRESSION_TYPE: lz4
```

**Low-latency profile:**
```yaml
SPRING_KAFKA_STREAMS_COMMIT_INTERVAL_MS: 100
SPRING_KAFKA_STREAMS_CACHE_MAX_BYTES_BUFFERING: 0
SPRING_KAFKA_STREAMS_POLL_MS: 10
SPRING_KAFKA_STREAMS_LINGER_MS: 0
```
