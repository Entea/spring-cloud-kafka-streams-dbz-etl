package com.example.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

public class CdcPipelineIT {

    private static final Logger log = LoggerFactory.getLogger(CdcPipelineIT.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private static final String EVENT_DETAILS_TOPIC = "event-details";
    private static final String DEBEZIUM_TOPIC = "dbserver1.public.event";

    private static Network network;
    private static PostgreSQLContainer<?> postgres;
    private static KafkaContainer kafka;
    private static GenericContainer<?> kafkaConnect;
    private static GenericContainer<?> app;
    private static GenericContainer<?> transformer;

    @BeforeAll
    static void startContainers() throws Exception {
        network = Network.newNetwork();

        // Start PostgreSQL with logical replication enabled
        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"))
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withDatabaseName("eventdb")
                .withUsername("postgres")
                .withPassword("postgres")
                .withCommand("postgres", "-c", "wal_level=logical")
                .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("postgres"));
        postgres.start();

        // Start Kafka (includes Zookeeper via KRaft)
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("kafka"));
        kafka.start();

        // Start Kafka Connect (Debezium)
        kafkaConnect = new GenericContainer<>(DockerImageName.parse("debezium/connect:2.4"))
                .withNetwork(network)
                .withNetworkAliases("kafka-connect")
                .withExposedPorts(8083)
                .withEnv("BOOTSTRAP_SERVERS", "kafka:9092")
                .withEnv("GROUP_ID", "connect-cluster")
                .withEnv("CONFIG_STORAGE_TOPIC", "connect-configs")
                .withEnv("OFFSET_STORAGE_TOPIC", "connect-offsets")
                .withEnv("STATUS_STORAGE_TOPIC", "connect-status")
                .withEnv("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("KEY_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                .waitingFor(Wait.forHttp("/connectors").forPort(8083).forStatusCode(200))
                .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("kafka-connect"));
        kafkaConnect.start();

        // Build and start the app container
        Path projectRoot = Path.of("..").toAbsolutePath().normalize();
        app = new GenericContainer<>(
                new ImageFromDockerfile("app-test", false)
                        .withDockerfile(projectRoot.resolve("app/Dockerfile"))
                        .withFileFromPath(".", projectRoot))
                .withNetwork(network)
                .withNetworkAliases("app")
                .withExposedPorts(8080)
                .withEnv("SPRING_DATASOURCE_URL", "jdbc:postgresql://postgres:5432/eventdb")
                .withEnv("SPRING_DATASOURCE_USERNAME", "postgres")
                .withEnv("SPRING_DATASOURCE_PASSWORD", "postgres")
                .waitingFor(Wait.forHttp("/api/events").forPort(8080).forStatusCode(200))
                .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("app"));
        app.start();

        // Build and start the transformer container
        transformer = new GenericContainer<>(
                new ImageFromDockerfile("transformer-test", false)
                        .withDockerfile(projectRoot.resolve("transformer/Dockerfile"))
                        .withFileFromPath(".", projectRoot))
                .withNetwork(network)
                .withNetworkAliases("transformer")
                .withExposedPorts(8081)
                .withEnv("SPRING_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
                .withEnv("APP_SERVICE_URL", "http://app:8080")
                .waitingFor(Wait.forListeningPort())
                .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("transformer"));
        transformer.start();

        // Register Debezium connector
        registerDebeziumConnector();

        // Wait for connector to be running
        waitForConnectorRunning();
    }

    @AfterAll
    static void stopContainers() {
        if (transformer != null) transformer.stop();
        if (app != null) app.stop();
        if (kafkaConnect != null) kafkaConnect.stop();
        if (kafka != null) kafka.stop();
        if (postgres != null) postgres.stop();
        if (network != null) network.close();
    }

    @Test
    void testCreateAndUpdateEventAppearsInKafkaTopic() throws Exception {
        // Create a Kafka consumer to listen to the event-details topic
        List<JsonNode> receivedEvents = Collections.synchronizedList(new ArrayList<>());

        try (KafkaConsumer<String, String> consumer = createKafkaConsumer()) {
            consumer.subscribe(Collections.singletonList(EVENT_DETAILS_TOPIC));

            // Start consuming in a background thread
            Thread consumerThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            JsonNode event = objectMapper.readTree(record.value());
                            log.info("Received event: {}", event);
                            receivedEvents.add(event);
                        } catch (Exception e) {
                            log.error("Failed to parse event", e);
                        }
                    }
                }
            });
            consumerThread.start();

            // Create an event via REST API
            String appUrl = "http://localhost:" + app.getMappedPort(8080);
            log.info("Creating event via REST API at {}", appUrl);

            JsonNode createdEvent = createEvent(appUrl, "test-event-1");
            assertNotNull(createdEvent);
            long eventId = createdEvent.get("id").asLong();
            log.info("Created event with ID: {}", eventId);

            // Wait for the created event to appear in Kafka
            await().atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(() -> receivedEvents.stream()
                            .anyMatch(e -> e.get("id").asLong() == eventId
                                    && "test-event-1".equals(e.get("name").asText())));

            log.info("Create event appeared in Kafka topic");

            // Update the event via REST API
            JsonNode updatedEvent = updateEvent(appUrl, eventId, "test-event-1-updated");
            assertNotNull(updatedEvent);
            assertEquals("test-event-1-updated", updatedEvent.get("name").asText());
            log.info("Updated event: {}", updatedEvent);

            // Wait for the updated event to appear in Kafka
            await().atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(() -> receivedEvents.stream()
                            .anyMatch(e -> e.get("id").asLong() == eventId
                                    && "test-event-1-updated".equals(e.get("name").asText())));

            log.info("Update event appeared in Kafka topic");

            // Stop consumer thread
            consumerThread.interrupt();
            consumerThread.join(5000);
        }

        // Verify we received both events
        assertTrue(receivedEvents.stream()
                .anyMatch(e -> "test-event-1".equals(e.get("name").asText())),
                "Should have received the created event");
        assertTrue(receivedEvents.stream()
                .anyMatch(e -> "test-event-1-updated".equals(e.get("name").asText())),
                "Should have received the updated event");

        log.info("Test completed successfully! Received {} events total", receivedEvents.size());
    }

    private static void registerDebeziumConnector() throws IOException, InterruptedException {
        String connectorConfig = """
                {
                  "name": "event-connector",
                  "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "database.hostname": "postgres",
                    "database.port": "5432",
                    "database.user": "postgres",
                    "database.password": "postgres",
                    "database.dbname": "eventdb",
                    "topic.prefix": "dbserver1",
                    "table.include.list": "public.event",
                    "plugin.name": "pgoutput",
                    "slot.name": "event_slot",
                    "publication.name": "event_publication",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false"
                  }
                }
                """;

        String connectUrl = "http://localhost:" + kafkaConnect.getMappedPort(8083);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(connectUrl + "/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(connectorConfig))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        log.info("Debezium connector registration response: {} - {}", response.statusCode(), response.body());
        assertTrue(response.statusCode() == 201 || response.statusCode() == 409,
                "Failed to register connector: " + response.body());
    }

    private static void waitForConnectorRunning() {
        String connectUrl = "http://localhost:" + kafkaConnect.getMappedPort(8083);
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(connectUrl + "/connectors/event-connector/status"))
                                .GET()
                                .build();
                        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() == 200) {
                            JsonNode status = objectMapper.readTree(response.body());
                            String state = status.path("connector").path("state").asText();
                            log.info("Connector state: {}", state);
                            return "RUNNING".equals(state);
                        }
                    } catch (Exception e) {
                        log.debug("Waiting for connector: {}", e.getMessage());
                    }
                    return false;
                });
        log.info("Debezium connector is running");
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "integration-test-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(props);
    }

    private JsonNode createEvent(String appUrl, String name) throws IOException, InterruptedException {
        String body = String.format("{\"name\": \"%s\"}", name);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(appUrl + "/api/events"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Failed to create event: " + response.body());
        return objectMapper.readTree(response.body());
    }

    private JsonNode updateEvent(String appUrl, long id, String newName) throws IOException, InterruptedException {
        String body = String.format("{\"name\": \"%s\"}", newName);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(appUrl + "/api/events/" + id))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Failed to update event: " + response.body());
        return objectMapper.readTree(response.body());
    }
}
