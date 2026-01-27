# Kafka Connect ETL

CDC-пайплайн на основе Debezium и Kafka Streams. Приложение захватывает изменения в PostgreSQL, обогащает их через REST API и публикует результат в выходные топики Kafka. Все сообщения сериализуются в Avro через Confluent Schema Registry.

## Архитектура

```
                              ┌─► [dbserver1.public.event] ─► [eventTransform] ─► [event-details]
[PostgreSQL] ─► [Debezium CDC]│                                    │ (ошибка)
                              │                                    └─► [event-transformer-dlq]
                              └─► [dbserver1.public.animal] ─► [animalTransform] ─► [animal-details]
                                                                    │ (ошибка)
                                                                    └─► [animal-transformer-dlq]

[animal-transformer-dlq] ─► [animalDlqTransform] ─► [animal-details]
                             (запуск через REST API)
```

Все топики используют Avro-сериализацию для ключей и значений. Debezium автоматически регистрирует схемы CDC-сообщений в Schema Registry. Transformer производит сообщения с генерированными Avro-типами (`RecordKey`, `EventDetails`, `AnimalDetails`).

### Модули

- **app** (порт 8080) — Spring Boot REST API с сущностями Event и Animal. CRUD-операции, миграции через Liquibase.
- **transformer** (порт 8081) — Spring Cloud Stream приложение с Kafka Streams. Читает CDC-топики, обогащает данные через запрос к `app` и публикует в выходные топики. Поддерживает DLQ и ручной экспорт.
- **integration-tests** — E2E-тесты на Testcontainers.

### Обработка ошибок (DLQ)

Если обогащение записи завершается ошибкой, она попадает в DLQ-топик (`event-transformer-dlq` или `animal-transformer-dlq`). Для повторной обработки animal DLQ есть отдельный стрим `animalDlqTransform`, который можно запустить и остановить через REST:

```bash
# Запустить обработку DLQ
curl -X POST http://localhost:8081/api/dlq/animal/start

# Остановить
curl -X POST http://localhost:8081/api/dlq/animal/stop
```

## Стек технологий

Java 21, Spring Boot 3.4, Spring Cloud Stream (Kafka Streams binder), PostgreSQL 16, Kafka (Confluent 7.5), Debezium 2.4, Confluent Schema Registry 7.5, Avro

## Сборка и запуск

### Сборка

```bash
# Собрать все модули
mvn clean package

# Собрать без тестов
mvn clean package -DskipTests

# Собрать Docker-образы (app и transformer)
./build-images.sh
```

### Запуск

```bash
# Поднять всю инфраструктуру
docker compose up -d
```

Порядок запуска: PostgreSQL + Zookeeper → Kafka → Schema Registry → app (миграции) → Kafka Connect → регистрация коннектора → transformer.

### Порты

| Сервис | Внутренний порт | Внешний порт |
|--------|----------------|--------------|
| app | 8080 | 8082 |
| transformer | 8081 | 8081 |
| Kafka Connect | 8083 | 8084 |
| Kafka UI | 8080 | 8085 |
| Schema Registry | 8081 | 8086 |

Проверка статуса коннектора:

```bash
curl http://localhost:8084/connectors/event-connector/status
```

Просмотр зарегистрированных Avro-схем:

```bash
curl http://localhost:8086/subjects
```

## Проверка работы

### 1. Создание данных

```bash
# Создать событие
curl -X POST http://localhost:8082/api/events \
  -H "Content-Type: application/json" \
  -d '{"name": "test-event"}'

# Создать животное
curl -X POST http://localhost:8082/api/animals \
  -H "Content-Type: application/json" \
  -d '{"name": "Buddy", "breed": "Golden Retriever"}'
```

### 2. Чтение выходных топиков

Поскольку сообщения сериализованы в Avro, `kafka-console-consumer` не может их прочитать напрямую. Используйте Kafka UI на `http://localhost:8085` или `kafka-avro-console-consumer`:

```bash
docker exec -it schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic event-details \
  --from-beginning \
  --property schema.registry.url=http://localhost:8081 \
  --property print.key=true \
  --key-deserializer io.confluent.kafka.serializers.KafkaAvroDeserializer \
  --property key.schema.registry.url=http://localhost:8081

docker exec -it schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic animal-details \
  --from-beginning \
  --property schema.registry.url=http://localhost:8081 \
  --property print.key=true \
  --key-deserializer io.confluent.kafka.serializers.KafkaAvroDeserializer \
  --property key.schema.registry.url=http://localhost:8081
```

### 3. Ручной экспорт животного

Можно отправить JSON-payload напрямую в transformer, минуя Debezium:

```bash
curl -X POST http://localhost:8081/api/export/animal \
  -H "Content-Type: application/json" \
  -d '{"id": 1, "version": 0, "name": "Buddy", "breed": "Golden Retriever"}'
```

### 4. Просмотр DLQ

```bash
docker exec -it schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic animal-transformer-dlq \
  --from-beginning \
  --property schema.registry.url=http://localhost:8081
```

## Avro-схемы

Схемы хранятся в `avro/` и компилируются в Java-классы через `avro-maven-plugin`:

- `avro/record_key.avsc` — ключ записи (`RecordKey` с полем `id`)
- `avro/event_details.avsc` — обогащённое событие (`EventDetails`)
- `avro/animal_details.avsc` — обогащённое животное (`AnimalDetails`)

Debezium автоматически регистрирует свои схемы (envelope с `before`/`after`) в Schema Registry.

## Настройка производительности

Параметры передаются через переменные окружения в `docker-compose.yml`.

### Высокая пропускная способность

```yaml
SPRING_KAFKA_STREAMS_NUM_STREAM_THREADS: 4
SPRING_KAFKA_STREAMS_COMMIT_INTERVAL_MS: 5000
SPRING_KAFKA_STREAMS_BATCH_SIZE: 65536
SPRING_KAFKA_STREAMS_LINGER_MS: 200
SPRING_KAFKA_STREAMS_COMPRESSION_TYPE: lz4
```

### Низкая задержка

```yaml
SPRING_KAFKA_STREAMS_COMMIT_INTERVAL_MS: 100
SPRING_KAFKA_STREAMS_CACHE_MAX_BYTES_BUFFERING: 0
SPRING_KAFKA_STREAMS_POLL_MS: 10
SPRING_KAFKA_STREAMS_LINGER_MS: 0
```

### Масштабирование

Для горизонтального масштабирования запустите несколько экземпляров transformer с одинаковым `application-id`. Kafka Streams автоматически распределит партиции. Максимальный параллелизм = количество партиций входного топика.
