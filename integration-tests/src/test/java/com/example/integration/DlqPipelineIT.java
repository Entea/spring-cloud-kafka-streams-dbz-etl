package com.example.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

public class DlqPipelineIT {

    private static final Logger log = LoggerFactory.getLogger(DlqPipelineIT.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private static final String ANIMAL_DETAILS_TOPIC = "animal-details";
    private static final String DLQ_TOPIC = "animal-transformer-dlq";

    private static final int KAFKA_INTERNAL_PORT = 9092;  // Used by containers in the same network
    private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;
    private static final int KAFKA_CONNECT_PORT = 8083;

    private static final int KAFKA_EXTERNAL_PORT = 29092;

    private static Network network;
    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> zookeeper;
    private static GenericContainer<?> kafka;
    private static GenericContainer<?> schemaRegistry;
    private static GenericContainer<?> kafkaConnect;
    private static GenericContainer<?> app;
    private static GenericContainer<?> transformer;

    @BeforeAll
    static void startContainers() throws Exception {
        network = Network.newNetwork();

        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"))
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withDatabaseName("eventdb")
                .withUsername("postgres")
                .withPassword("postgres")
                .withCommand("postgres", "-c", "wal_level=logical")
                .waitingFor(Wait.forListeningPort());
        postgres.start();
        log.info("PostgreSQL started");

        // Start Zookeeper
        zookeeper = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper:7.5.0"))
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        zookeeper.start();
        log.info("Zookeeper started");

        // Kafka startup script that waits for the advertised listeners config file
        String kafkaStartupScript =
                "echo 'Waiting for kafka_listeners config file...'; " +
                "while [ ! -f /tmp/kafka_listeners ]; do sleep 0.1; done; " +
                "export KAFKA_ADVERTISED_LISTENERS=$(cat /tmp/kafka_listeners); " +
                "echo 'Starting Kafka with KAFKA_ADVERTISED_LISTENERS='$KAFKA_ADVERTISED_LISTENERS; " +
                "/etc/confluent/docker/run";

        kafka = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withExposedPorts(KAFKA_EXTERNAL_PORT)
                .withEnv("KAFKA_BROKER_ID", "1")
                .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
                .withEnv("KAFKA_LISTENERS", "INTERNAL://0.0.0.0:" + KAFKA_INTERNAL_PORT + ",EXTERNAL://0.0.0.0:" + KAFKA_EXTERNAL_PORT)
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
                .withCommand("sh", "-c", kafkaStartupScript)
                .waitingFor(Wait.forLogMessage(".*Waiting for kafka_listeners.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(30)));
        kafka.start();

        // Inject the advertised listeners with the actual mapped port
        int mappedKafkaPort = kafka.getMappedPort(KAFKA_EXTERNAL_PORT);
        String advertisedListeners = "INTERNAL://kafka:" + KAFKA_INTERNAL_PORT + ",EXTERNAL://localhost:" + mappedKafkaPort;
        log.info("Writing Kafka advertised listeners: {}", advertisedListeners);
        kafka.execInContainer("sh", "-c", "echo '" + advertisedListeners + "' > /tmp/kafka_listeners");

        // Wait for Kafka to be ready
        await().atMost(120, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(() -> {
            try {
                var result = kafka.execInContainer("kafka-broker-api-versions", "--bootstrap-server", "localhost:" + KAFKA_INTERNAL_PORT);
                return result.getExitCode() == 0;
            } catch (Exception e) {
                return false;
            }
        });
        log.info("Kafka started, external port: {}", mappedKafkaPort);

        schemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.0"))
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:" + KAFKA_INTERNAL_PORT)  // KafkaContainer exposes BROKER listener on port 9092
                .waitingFor(Wait.forHttp("/subjects").forPort(SCHEMA_REGISTRY_INTERNAL_PORT).withStartupTimeout(Duration.ofMinutes(2)));
        schemaRegistry.start();
        log.info("Schema Registry started");

        // Build Kafka Connect image with Avro converters from project Dockerfile
        kafkaConnect = new GenericContainer<>(new ImageFromDockerfile()
                .withDockerfile(Path.of("../docker/connect/Dockerfile")))
                .withNetwork(network)
                .withNetworkAliases("kafka-connect")
                .withExposedPorts(KAFKA_CONNECT_PORT)
                .withEnv("BOOTSTRAP_SERVERS", "kafka:" + KAFKA_INTERNAL_PORT)
                .withEnv("GROUP_ID", "connect-cluster")
                .withEnv("CONFIG_STORAGE_TOPIC", "connect-configs")
                .withEnv("OFFSET_STORAGE_TOPIC", "connect-offsets")
                .withEnv("STATUS_STORAGE_TOPIC", "connect-status")
                .withEnv("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                .withEnv("KEY_CONVERTER_SCHEMA_REGISTRY_URL", "http://schema-registry:" + SCHEMA_REGISTRY_INTERNAL_PORT)
                .withEnv("VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                .withEnv("VALUE_CONVERTER_SCHEMA_REGISTRY_URL", "http://schema-registry:" + SCHEMA_REGISTRY_INTERNAL_PORT)
                // AvroConverter needs schema.registry.url - try different naming patterns
                .withEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:" + SCHEMA_REGISTRY_INTERNAL_PORT)
                .withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", "http://schema-registry:" + SCHEMA_REGISTRY_INTERNAL_PORT)
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", "http://schema-registry:" + SCHEMA_REGISTRY_INTERNAL_PORT)
                .waitingFor(Wait.forHttp("/connectors").forPort(KAFKA_CONNECT_PORT).withStartupTimeout(Duration.ofMinutes(3)));
        kafkaConnect.start();
        log.info("Kafka Connect started");

        app = new GenericContainer<>(new ImageFromDockerfile()
                .withFileFromPath("target/app-1.0.0-SNAPSHOT.jar", Path.of("../app/target/app-1.0.0-SNAPSHOT.jar"))
                .withDockerfileFromBuilder(builder -> builder.from("eclipse-temurin:21-jre-jammy")
                        .workDir("/app")
                        .copy("target/app-1.0.0-SNAPSHOT.jar", "app.jar")
                        .cmd("java", "-jar", "app.jar").build()))
                .withNetwork(network)
                .withNetworkAliases("app")
                .withExposedPorts(8080)
                .withEnv("SPRING_DATASOURCE_URL", "jdbc:postgresql://postgres:5432/eventdb")
                .withEnv("SPRING_DATASOURCE_USERNAME", "postgres")
                .withEnv("SPRING_DATASOURCE_PASSWORD", "postgres")
                .waitingFor(Wait.forHttp("/api/animals").forPort(8080).withStartupTimeout(Duration.ofMinutes(2)));
        app.start();
        log.info("App started");

        transformer = new GenericContainer<>(new ImageFromDockerfile()
                .withFileFromPath("target/transformer-1.0.0-SNAPSHOT.jar", Path.of("../transformer/target/transformer-1.0.0-SNAPSHOT.jar"))
                .withDockerfileFromBuilder(builder -> builder.from("eclipse-temurin:21-jre-jammy")
                        .workDir("/app")
                        .copy("target/transformer-1.0.0-SNAPSHOT.jar", "transformer.jar")
                        .cmd("java", "-jar", "transformer.jar").build()))
                .withNetwork(network)
                .withNetworkAliases("transformer")
                .withExposedPorts(8081)
                .withEnv("SPRING_KAFKA_BOOTSTRAP_SERVERS", "kafka:" + KAFKA_INTERNAL_PORT)
                .withEnv("SPRING_CLOUD_STREAM_KAFKA_STREAMS_BINDER_CONFIGURATION_SCHEMA_REGISTRY_URL", "http://schema-registry:" + SCHEMA_REGISTRY_INTERNAL_PORT)
                .withEnv("APP_SERVICE_URL", "http://app:9999") // Broken URL
                .withEnv("APP_REPAIR_SERVICE_URL", "http://app:8080") // Correct URL
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        transformer.start();
        log.info("Transformer started");

        // Wait for transformer to fully initialize Kafka Streams
        log.info("Waiting for transformer to initialize...");
        Thread.sleep(10000);

        registerDebeziumConnector();
        waitForConnectorRunning();

        // List topics for debugging
        try {
            var result = kafka.execInContainer("kafka-topics", "--bootstrap-server", "localhost:9092", "--list");
            log.info("Available Kafka topics: {}", result.getStdout());
        } catch (Exception e) {
            log.warn("Could not list Kafka topics", e);
        }
    }

    @AfterAll
    static void stopContainers() {
        log.info("Stopping containers...");
        if (transformer != null) transformer.stop();
        if (app != null) app.stop();
        if (kafkaConnect != null) kafkaConnect.stop();
        if (schemaRegistry != null) schemaRegistry.stop();
        if (kafka != null) kafka.stop();
        if (zookeeper != null) zookeeper.stop();
        if (postgres != null) postgres.stop();
        if (network != null) network.close();
        log.info("Containers stopped");
    }

    private static final String CDC_TOPIC = "dbserver1.public.animal";

    @Test
    void testAnimalDlqProcessing() throws Exception {
        log.info("PHASE 1: Triggering DLQ...");
        long failedAnimalId = createAnimal("FailDog", "Unknown");

        // First, verify CDC topic received the message
        log.info("Checking CDC topic {} for animal {}", CDC_TOPIC, failedAnimalId);
        try (KafkaConsumer<GenericRecord, GenericRecord> cdcConsumer = createAvroConsumer(CDC_TOPIC)) {
            await().atMost(60, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(() -> {
                ConsumerRecords<GenericRecord, GenericRecord> records = cdcConsumer.poll(Duration.ofMillis(1000));
                log.info("CDC topic poll returned {} records", records.count());
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    log.info("CDC record key: {}, value schema: {}", record.key(),
                            record.value() != null ? record.value().getSchema().getName() : "null");
                    if (record.value() != null && record.value().get("after") instanceof GenericRecord after) {
                        long id = ((Number) after.get("id")).longValue();
                        log.info("Found CDC record for animal ID: {}", id);
                        if (id == failedAnimalId) {
                            return true;
                        }
                    }
                }
                return false;
            });
        }
        log.info("CDC message confirmed for animal {}", failedAnimalId);

        // Check what topics exist now
        try {
            var result = kafka.execInContainer("kafka-topics", "--bootstrap-server", "localhost:9092", "--list");
            log.info("Topics after CDC: {}", result.getStdout());
        } catch (Exception e) {
            log.warn("Could not list topics", e);
        }

        // Also check animal-details topic to see if messages went there instead
        log.info("Checking animal-details topic for any messages");
        try (KafkaConsumer<GenericRecord, GenericRecord> detailsConsumer = createAvroConsumer(ANIMAL_DETAILS_TOPIC)) {
            ConsumerRecords<GenericRecord, GenericRecord> detailsRecords = detailsConsumer.poll(Duration.ofSeconds(5));
            log.info("animal-details topic has {} records", detailsRecords.count());
            for (ConsumerRecord<GenericRecord, GenericRecord> record : detailsRecords) {
                log.info("animal-details record: {}", record.value());
            }
        }

        // Print transformer container logs for debugging
        log.info("Transformer container logs (last 50 lines):");
        String logs = transformer.getLogs();
        String[] logLines = logs.split("\n");
        int startIdx = Math.max(0, logLines.length - 50);
        for (int i = startIdx; i < logLines.length; i++) {
            log.info("  TRANSFORMER: {}", logLines[i]);
        }

        // Now check DLQ topic
        log.info("Checking DLQ topic {} for animal {}", DLQ_TOPIC, failedAnimalId);
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = createAvroConsumer(DLQ_TOPIC)) {
            await().atMost(60, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(() -> {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                log.info("DLQ topic poll returned {} records", records.count());
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    log.info("DLQ record: key={}, value schema={}", record.key(),
                            record.value() != null ? record.value().getSchema().getName() : "null");
                    if (isMatchingRecord(record, failedAnimalId)) {
                        log.info("Verified message for animal {} in DLQ topic.", failedAnimalId);
                        return true;
                    }
                }
                return false;
            });
        }
        log.info("DLQ message confirmed for animal {}", failedAnimalId);

        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = createAvroConsumer(ANIMAL_DETAILS_TOPIC)) {
            ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                if (isMatchingRecord(record, failedAnimalId)) {
                    fail("Message for animal " + failedAnimalId + " should not be in final topic yet.");
                }
            }
            log.info("Verified message for animal {} is NOT in final topic.", failedAnimalId);
        }

        log.info("PHASE 2: Starting DLQ reprocessing...");
        controlDlqStream("start");

        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = createAvroConsumer(ANIMAL_DETAILS_TOPIC)) {
            await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    if (isMatchingRecord(record, failedAnimalId)) {
                        assertEquals("FailDog", record.value().get("name").toString());
                        log.info("Verified reprocessed message for animal {} in final topic.", failedAnimalId);
                        return true;
                    }
                }
                return false;
            });
        }

        log.info("PHASE 3: Stopping DLQ stream and verifying...");
        controlDlqStream("stop");
        long anotherFailedId = createAnimal("StopTestDog", "Poodle");

        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = createAvroConsumer(DLQ_TOPIC)) {
            await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    if (isMatchingRecord(record, anotherFailedId)) {
                        log.info("Verified second message for animal {} in DLQ topic.", anotherFailedId);
                        return true;
                    }
                }
                return false;
            });
        }

        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = createAvroConsumer(ANIMAL_DETAILS_TOPIC)) {
            ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                if (isMatchingRecord(record, anotherFailedId)) {
                    fail("Message for " + anotherFailedId + " should not be processed after stopping DLQ stream.");
                }
            }
            log.info("Verified second message for animal {} is NOT in final topic.", anotherFailedId);
        }
    }

    private boolean isMatchingRecord(ConsumerRecord<GenericRecord, GenericRecord> record, long expectedId) {
        GenericRecord key = record.key();
        GenericRecord value = record.value();
        // Check if the key is the enriched key from the final topic
        if (key != null && key.getSchema().getName().equals("RecordKey")) {
            return ((Number) key.get("id")).longValue() == expectedId;
        }
        // Check if the value is the original Debezium message from the DLQ
        if (value != null && value.get("after") instanceof GenericRecord after) {
            return ((Number) after.get("id")).longValue() == expectedId;
        }
        return false;
    }

    private static String getKafkaConnectUrl() { return "http://" + kafkaConnect.getHost() + ":" + kafkaConnect.getMappedPort(KAFKA_CONNECT_PORT); }
    private static String getAppUrl() { return "http://" + app.getHost() + ":" + app.getMappedPort(8080); }
    private static String getTransformerUrl() { return "http://" + transformer.getHost() + ":" + transformer.getMappedPort(8081); }
    private static String getSchemaRegistryUrlExternal() { return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT); }
    private static String getKafkaBootstrapServersExternal() { return "localhost:" + kafka.getMappedPort(KAFKA_EXTERNAL_PORT); }

    private static void registerDebeziumConnector() throws Exception {
        postgres.execInContainer("psql", "-U", "postgres", "-d", "eventdb", "-c", "CREATE PUBLICATION animal_publication FOR TABLE public.animal;");

        String connectorConfig = String.format("""
                {
                  \"name\": \"animal-connector\",
                  \"config\": {
                    \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
                    \"database.hostname\": \"postgres\", \"database.port\": \"5432\", \"database.user\": \"postgres\",
                    \"database.password\": \"postgres\", \"database.dbname\": \"eventdb\",
                    \"topic.prefix\": \"dbserver1\", \"table.include.list\": \"public.animal\",
                    \"plugin.name\": \"pgoutput\", \"slot.name\": \"animal_slot\", \"publication.name\": \"animal_publication\"
                  }
                }""");

        HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create(getKafkaConnectUrl() + "/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(connectorConfig)).build(), HttpResponse.BodyHandlers.ofString());
        log.info("Debezium connector registration response: {} - {}", response.statusCode(), response.body());
        assertTrue(response.statusCode() == 201 || response.statusCode() == 409, "Failed to register connector");
    }

    private static void waitForConnectorRunning() {
        await().atMost(90, TimeUnit.SECONDS).pollInterval(3, TimeUnit.SECONDS).until(() -> {
            try {
                HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                        .uri(URI.create(getKafkaConnectUrl() + "/connectors/animal-connector/status")).build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    var status = objectMapper.readTree(response.body());
                    String connectorState = status.path("connector").path("state").asText();
                    var tasks = status.path("tasks");
                    log.info("Connector state: {}, tasks: {}", connectorState, tasks);
                    if ("RUNNING".equals(connectorState) && tasks.size() > 0 &&
                            "RUNNING".equals(tasks.get(0).path("state").asText())) {
                        log.info("Debezium connector and task are running");
                        return true;
                    }
                }
            } catch (Exception e) {
                log.debug("Waiting for connector: {}", e.getMessage());
            }
            return false;
        });
    }

    private KafkaConsumer<GenericRecord, GenericRecord> createAvroConsumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBootstrapServersExternal());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-consumer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrlExternal());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    private long createAnimal(String name, String breed) throws IOException, InterruptedException {
        String body = String.format("{\"name\": \"%s\", \"breed\": \"%s\"}", name, breed);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getAppUrl() + "/api/animals"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        long id = objectMapper.readTree(response.body()).get("id").asLong();
        log.info("Created animal with ID: {}", id);
        return id;
    }

    private void controlDlqStream(String action) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getTransformerUrl() + "/api/dlq/animal/" + action))
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        log.info("Sent '{}' command to DLQ stream. Response: {}", action, response.body());
    }
}