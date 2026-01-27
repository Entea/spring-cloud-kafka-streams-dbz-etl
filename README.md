# Kafka Connect ETL

CDC-пайплайн на основе Debezium и Kafka Streams. Приложение захватывает изменения в PostgreSQL, обогащает их через REST API и публикует результат в выходные топики Kafka.

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

Java 21, Spring Boot 3.4, Spring Cloud Stream (Kafka Streams binder), PostgreSQL 16, Kafka (Confluent 7.5), Debezium 2.4

## Сборка и запуск

### Сборка

```bash
# Собрать все модули
mvn clean package

# Собрать без тестов
mvn clean package -DskipTests

# Собрать Docker-образы
./build-images.sh
```

### Запуск

```bash
# Поднять всю инфраструктуру
docker-compose up -d
```

Порядок запуска: PostgreSQL + Zookeeper → Kafka → app (миграции) → Kafka Connect → регистрация коннектора → transformer.

Проверка статуса коннектора:

```bash
curl http://localhost:8083/connectors/event-connector/status
```

## Проверка работы

### 1. Создание данных

```bash
# Создать событие
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{"name": "test-event"}'

# Создать животное
curl -X POST http://localhost:8080/api/animals \
  -H "Content-Type: application/json" \
  -d '{"name": "Buddy", "breed": "Golden Retriever"}'
```

### 2. Чтение выходных топиков

```bash
# Обогащённые события
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic event-details \
  --from-beginning

# Обогащённые животные
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic animal-details \
  --from-beginning
```

### 3. Ручной экспорт животного

Можно отправить CDC-payload напрямую в transformer, минуя Debezium:

```bash
curl -X POST http://localhost:8081/api/export/animal \
  -H "Content-Type: application/json" \
  -d '{"after":{"id":1}}'
```

### 4. Просмотр DLQ

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic animal-transformer-dlq \
  --from-beginning
```

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
