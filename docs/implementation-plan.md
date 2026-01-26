# Kafka Connect ETL Application - Implementation Plan

## Overview
Create a Maven multi-module Kafka Connect ETL application with:
- **app**: Spring Boot 3.4.x REST API with Event entity
- **transformer**: Kafka Streams app that enriches events
- **Docker Compose**: Full infrastructure (Postgres, Kafka, Debezium, apps)

## Architecture Flow
```
[PostgreSQL] --> [Debezium CDC] --> [Kafka: dbserver1.public.event]
                                           |
                                           v (event IDs)
                                    [Transformer App]
                                           | (REST call to fetch full event)
                                           v
                                    [Kafka: event-details]
```

---

## Project Structure

```
kafka-connect-etl/
├── pom.xml                          # Parent POM
├── app/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/
│       ├── java/com/example/app/
│       │   ├── AppApplication.java
│       │   ├── entity/Event.java
│       │   ├── repository/EventRepository.java
│       │   └── controller/EventController.java
│       └── resources/
│           ├── application.yml
│           └── db/changelog/
│               ├── db.changelog-master.yaml
│               └── 001-create-event-table.yaml
├── transformer/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/
│       ├── java/com/example/transformer/
│       │   ├── TransformerApplication.java
│       │   ├── config/KafkaStreamsConfig.java
│       │   └── stream/EventTransformerStream.java
│       └── resources/
│           └── application.yml
└── docker/
    ├── docker-compose.yml
    └── connect/
        └── register-connector.json
```

---

## Implementation Steps

### Step 1: Parent POM
**File**: `pom.xml`
- Maven multi-module project
- Define common properties (Java 21, Spring Boot 3.4.x)
- Modules: `app`, `transformer`

### Step 2: App Module

#### 2.1 App POM
**File**: `app/pom.xml`
- Dependencies: spring-boot-starter-web, spring-boot-starter-data-jpa, liquibase-core, postgresql

#### 2.2 Event Entity
**File**: `app/src/main/java/com/example/app/entity/Event.java`
- Fields: `id` (Long, auto-generated), `version` (Long, @Version), `name` (String)
- JPA annotations

#### 2.3 Event Repository
**File**: `app/src/main/java/com/example/app/repository/EventRepository.java`
- Extends JpaRepository<Event, Long>

#### 2.4 Event Controller
**File**: `app/src/main/java/com/example/app/controller/EventController.java`
- REST CRUD endpoints:
  - `GET /api/events` - List all
  - `GET /api/events/{id}` - Get by ID
  - `POST /api/events` - Create
  - `PUT /api/events/{id}` - Update
  - `DELETE /api/events/{id}` - Delete

#### 2.5 Liquibase Changelog
**Files**:
- `db/changelog/db.changelog-master.yaml` - Master changelog
- `db/changelog/001-create-event-table.yaml` - Create event table with id, version, name

#### 2.6 Application Config
**File**: `app/src/main/resources/application.yml`
- PostgreSQL datasource config
- Liquibase config
- Server port: 8080

#### 2.7 App Dockerfile
**File**: `app/Dockerfile`
- Multi-stage build with Maven and Eclipse Temurin JDK 21

### Step 3: Transformer Module

#### 3.1 Transformer POM
**File**: `transformer/pom.xml`
- Dependencies: kafka-streams, spring-kafka, spring-boot-starter-web (for RestTemplate)

#### 3.2 Kafka Streams Config
**File**: `transformer/src/main/java/com/example/transformer/config/KafkaStreamsConfig.java`
- Configure Kafka Streams with application ID, bootstrap servers, serdes

#### 3.3 Event Transformer Stream
**File**: `transformer/src/main/java/com/example/transformer/stream/EventTransformerStream.java`
- Read from Debezium topic (CDC events with event IDs)
- Extract event ID from CDC payload
- Call app REST API individually: `GET /api/events/{id}`
- Transform and output to `event-details` topic as JSON

#### 3.4 Application Config
**File**: `transformer/src/main/resources/application.yml`
- Kafka bootstrap servers
- App service URL for REST calls
- Stream configs

#### 3.5 Transformer Dockerfile
**File**: `transformer/Dockerfile`
- Multi-stage build

### Step 4: Docker Compose Infrastructure

**File**: `docker/docker-compose.yml`

Services:
1. **zookeeper** - Kafka coordination
2. **kafka** - Message broker
3. **postgres** - Database for app
4. **kafka-connect** - Debezium connector runtime
5. **app** - Spring Boot REST API
6. **transformer** - Kafka Streams processor

#### 4.1 Debezium Connector Config
**File**: `docker/connect/register-connector.json`
- PostgreSQL connector configuration
- Monitor `public.event` table
- Capture `id` column changes

---

## Key Technical Decisions

1. **Spring Boot Version**: 3.4.x (latest stable)
2. **Java Version**: 21 (LTS)
3. **Debezium**: Captures CDC events from PostgreSQL WAL
4. **Kafka Streams**: Processes events individually, enriches via REST calls
5. **Topic Naming**:
   - Input: `dbserver1.public.event` (Debezium default)
   - Output: `event-details`

---

## Verification

1. Start infrastructure:
   ```bash
   cd docker && docker-compose up -d
   ```

2. Wait for services to be healthy, then register Debezium connector:
   ```bash
   curl -X POST -H "Content-Type: application/json" \
     --data @connect/register-connector.json \
     http://localhost:8083/connectors
   ```

3. Create an event via REST API:
   ```bash
   curl -X POST http://localhost:8080/api/events \
     -H "Content-Type: application/json" \
     -d '{"name": "test-event"}'
   ```

4. Check `event-details` topic for transformed output:
   ```bash
   docker exec -it kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic event-details \
     --from-beginning
   ```

Expected output in `event-details`:
```json
{"id": 1, "version": 0, "name": "test-event"}
```
