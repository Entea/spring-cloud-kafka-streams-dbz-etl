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
# Start all infrastructure (from docker/ directory)
cd docker && docker-compose up -d

# Register Debezium connector (after services are healthy)
curl -X POST -H "Content-Type: application/json" \
  --data @connect/register-connector.json \
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
