# Manual Testing: AnimalTransform Stream Offset Management API

## Prerequisites

1. Start the infrastructure:
```bash
docker-compose up -d
```

2. Wait for all services to be healthy:
```bash
docker-compose ps
```

3. Register the Debezium connector:
```bash
curl -X POST -H "Content-Type: application/json" \
  --data @docker/connect/register-connector.json \
  http://localhost:8083/connectors
```

4. Verify connector is running:
```bash
curl http://localhost:8083/connectors/event-connector/status | jq
```

## Test 1: Get Current Offsets

```bash
curl -s http://localhost:8081/api/stream/animal/offsets | jq
```

Expected response:
```json
{
  "consumerGroup": "animal-transformer",
  "topic": "dbserver1.public.animal",
  "partitionOffsets": [
    {
      "partition": 0,
      "committedOffset": 0,
      "endOffset": 0,
      "lag": 0
    }
  ],
  "timestamp": "2026-01-30T10:30:00Z"
}
```

## Test 2: Create Test Data

Create a few animals to generate CDC events:

```bash
# Create first animal
curl -X POST http://localhost:8080/api/animals \
  -H "Content-Type: application/json" \
  -d '{"name": "Buddy", "breed": "Golden Retriever"}'

# Create second animal
curl -X POST http://localhost:8080/api/animals \
  -H "Content-Type: application/json" \
  -d '{"name": "Max", "breed": "German Shepherd"}'

# Create third animal
curl -X POST http://localhost:8080/api/animals \
  -H "Content-Type: application/json" \
  -d '{"name": "Luna", "breed": "Labrador"}'
```

Wait a few seconds for CDC to process, then check offsets:

```bash
curl -s http://localhost:8081/api/stream/animal/offsets | jq
```

Expected: `committedOffset` should increase, `lag` should be 0 (all processed).

## Test 3: Get Stream Status

```bash
curl -s http://localhost:8081/api/stream/animal/status | jq
```

Expected response:
```json
{
  "applicationId": "animal-transformer",
  "running": true,
  "state": "RUNNING",
  "timestamp": "2026-01-30T10:35:00Z"
}
```

## Test 4: Stop/Start Stream

Stop the stream:

```bash
curl -s -X POST http://localhost:8081/api/stream/animal/stop | jq
```

Expected response:
```json
{
  "applicationId": "animal-transformer",
  "running": false,
  "state": "NOT_RUNNING",
  "timestamp": "2026-01-30T10:36:00Z"
}
```

Start the stream:

```bash
curl -s -X POST http://localhost:8081/api/stream/animal/start | jq
```

Expected response:
```json
{
  "applicationId": "animal-transformer",
  "running": true,
  "state": "RUNNING",
  "timestamp": "2026-01-30T10:37:00Z"
}
```

## Test 5: Offset Reset via API

The API automatically stops the stream, resets offsets, and restarts the stream.

### Reset to earliest offset:

```bash
curl -s -X POST http://localhost:8081/api/stream/animal/offsets/reset/earliest | jq
```

Expected response:
```json
{
  "success": true,
  "message": "Offsets reset successfully to earliest",
  "previousOffsets": {
    "0": 3
  },
  "newOffsets": {
    "0": 0
  }
}
```

### Reset to latest offset:

```bash
curl -s -X POST http://localhost:8081/api/stream/animal/offsets/reset/latest | jq
```

Expected response:
```json
{
  "success": true,
  "message": "Offsets reset successfully to latest",
  "previousOffsets": {
    "0": 0
  },
  "newOffsets": {
    "0": 3
  }
}
```

### Reset to specific offsets:

```bash
curl -s -X POST http://localhost:8081/api/stream/animal/offsets/reset \
  -H "Content-Type: application/json" \
  -d '{"partitionOffsets": {"0": 1}}' | jq
```

Expected response:
```json
{
  "success": true,
  "message": "Offsets reset successfully to specified values",
  "previousOffsets": {
    "0": 3
  },
  "newOffsets": {
    "0": 1
  }
}
```

### Verify offset was reset:

```bash
curl -s http://localhost:8081/api/stream/animal/offsets | jq
```

## Test 6: Verify Output Topic

Check that records are being written to the output topic:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic animal-details \
  --from-beginning \
  --max-messages 5
```

## Cleanup

```bash
docker-compose down -v
```

## Troubleshooting

### No data appearing
1. Check Debezium connector status:
   ```bash
   curl http://localhost:8083/connectors/event-connector/status | jq
   ```

2. Check if CDC topic has data:
   ```bash
   docker exec -it kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic dbserver1.public.animal \
     --from-beginning \
     --max-messages 1
   ```

3. Check transformer logs for errors:
   ```bash
   docker-compose logs transformer | grep -i error
   ```

## API Summary

| Method | Path | Status | Description |
|--------|------|--------|-------------|
| GET | `/api/stream/animal/offsets` | 200 | Get committed offsets and lag info |
| GET | `/api/stream/animal/status` | 200 | Get stream running status and state |
| POST | `/api/stream/animal/offsets/reset` | 200/409 | Reset to specific offsets (body: `{"partitionOffsets": {"0": 5}}`) |
| POST | `/api/stream/animal/offsets/reset/earliest` | 200/409 | Reset to earliest offsets |
| POST | `/api/stream/animal/offsets/reset/latest` | 200/409 | Reset to latest offsets |
| POST | `/api/stream/animal/stop` | 200 | Stop the Kafka Streams instance |
| POST | `/api/stream/animal/start` | 200 | Start the Kafka Streams instance |
