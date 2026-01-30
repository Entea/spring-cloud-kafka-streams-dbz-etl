# Manual Testing Guide

## Prerequisites

1. Build Docker images:

```bash
./build-images.sh
```

2. Start infrastructure:

```bash
docker-compose up -d
```

3. Wait for all services to become healthy:

```bash
docker-compose ps
```

All services should show `healthy` status. The connector is registered automatically by the `register-connector` service.

4. Verify connector is running:

```bash
curl http://localhost:8084/connectors/event-connector/status
```

### Service Ports

| Service         | Port |
|-----------------|------|
| App (REST API)  | 8082 |
| Transformer     | 8081 |
| Kafka (external)| 29092 |
| Kafka Connect   | 8084 |
| Schema Registry | 8086 |
| Kafka UI        | 8085 |

---

## 1. Event CDC Pipeline

### Create an event

```bash
curl -X POST http://localhost:8082/api/events \
  -H "Content-Type: application/json" \
  -d '{"name": "test-event"}'
```

### Verify message in `event-details`

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic event-details \
  --from-beginning \
  --max-messages 1
```

You should see the enriched event record.

### Update the event

```bash
curl -X PUT http://localhost:8082/api/events/1 \
  -H "Content-Type: application/json" \
  -d '{"name": "updated-event"}'
```

Consume from `event-details` again and verify the updated record appears.

---

## 2. Animal CDC Pipeline

### Create an animal

```bash
curl -X POST http://localhost:8082/api/animals \
  -H "Content-Type: application/json" \
  -d '{"name": "Buddy", "breed": "Golden Retriever"}'
```

### Verify message in `animal-details`

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic animal-details \
  --from-beginning \
  --max-messages 1
```

### Update the animal

```bash
curl -X PUT http://localhost:8082/api/animals/1 \
  -H "Content-Type: application/json" \
  -d '{"name": "Buddy", "breed": "Labrador"}'
```

Consume from `animal-details` again and verify the updated record appears.

---

## 3. DLQ Testing

### 3.1 Trigger DLQ

Break the main transformer's connection to the app by setting a wrong `APP_SERVICE_URL`, while keeping the repair URL correct. This will cause the main processing to fail and send messages to the DLQ, while allowing the DLQ processor to succeed later.

Stop the transformer:
```bash
docker-compose stop transformer
```

Edit `docker-compose.yml` and change **only** the transformer's `APP_SERVICE_URL` to an invalid URL. Ensure `APP_REPAIR_SERVICE_URL` remains correct.

```yaml
    environment:
      # ...
      APP_SERVICE_URL: http://app:9999 # This is the broken URL
      APP_REPAIR_SERVICE_URL: http://app:8080 # This remains correct for the DLQ processor
      # ...
```

Restart the transformer:

```bash
docker-compose up -d transformer
```

### 3.2 Create an animal that will fail enrichment

```bash
curl -X POST http://localhost:8082/api/animals \
  -H "Content-Type: application/json" \
  -d '{"name": "FailDog", "breed": "Unknown"}'
```

### 3.3 Verify message lands in DLQ

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic animal-transformer-dlq \
  --from-beginning \
  --max-messages 1
```

You should see the original CDC payload in the DLQ topic.

### 3.4 Reprocess from DLQ

Since the `APP_REPAIR_SERVICE_URL` is still correct, we can process the message from the DLQ without restarting the transformer.

Start DLQ reprocessing:

```bash
curl -X POST http://localhost:8081/api/dlq/animal/start
```

Expected response: `Animal DLQ stream started`

### 3.5 Verify reprocessed message in `animal-details`

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic animal-details \
  --from-beginning
```

The previously failed animal record for "FailDog" should now appear in `animal-details`.

### 3.6 Stop DLQ reprocessing

```bash
curl -X POST http://localhost:8081/api/dlq/animal/stop
```

Expected response: `Animal DLQ stream stopped`

### 3.7 Verify stop works

Create another animal that will fail:

```bash
curl -X POST http://localhost:8082/api/animals \
  -H "Content-Type: application/json" \
  -d '{"name": "AnotherDog", "breed": "Poodle"}'
```

This new message will land in `animal-transformer-dlq`. Since the DLQ stream is now stopped, it should **not** be reprocessed into `animal-details`. Confirm by consuming from `animal-details` and verifying no new record for "AnotherDog" appears.

Finally, to restore normal processing, revert the `APP_SERVICE_URL` in `docker-compose.yml` and restart the transformer.
```yaml
APP_SERVICE_URL: http://app:8080
```
```bash
docker-compose stop transformer
docker-compose up -d transformer
```
