package com.example.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.*;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AnimalStreamControlIT {

    private static final Logger log = LoggerFactory.getLogger(AnimalStreamControlIT.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private static final String ANIMAL_DETAILS_TOPIC = "animal-details";
    private static final String CDC_TOPIC = "dbserver1.public.animal";

    private static final int KAFKA_INTERNAL_PORT = 9092;
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

        zookeeper = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper:7.5.0"))
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        zookeeper.start();
        log.info("Zookeeper started");

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

        int mappedKafkaPort = kafka.getMappedPort(KAFKA_EXTERNAL_PORT);
        String advertisedListeners = "INTERNAL://kafka:" + KAFKA_INTERNAL_PORT + ",EXTERNAL://localhost:" + mappedKafkaPort;
        log.info("Writing Kafka advertised listeners: {}", advertisedListeners);
        kafka.execInContainer("sh", "-c", "echo '" + advertisedListeners + "' > /tmp/kafka_listeners");

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
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:" + KAFKA_INTERNAL_PORT)
                .waitingFor(Wait.forHttp("/subjects").forPort(SCHEMA_REGISTRY_INTERNAL_PORT).withStartupTimeout(Duration.ofMinutes(2)));
        schemaRegistry.start();
        log.info("Schema Registry started");

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
                .withEnv("SPRING_CLOUD_STREAM_KAFKA_STREAMS_BINDER_BROKERS", "kafka:" + KAFKA_INTERNAL_PORT)
                .withEnv("SPRING_CLOUD_STREAM_KAFKA_STREAMS_BINDER_CONFIGURATION_SCHEMA_REGISTRY_URL", "http://schema-registry:" + SCHEMA_REGISTRY_INTERNAL_PORT)
                .withEnv("APP_SERVICE_URL", "http://app:8080")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        transformer.start();
        log.info("Transformer started");

        log.info("Waiting for transformer to initialize...");
        Thread.sleep(10000);

        registerDebeziumConnector();
        waitForConnectorRunning();

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

    @Test
    @Order(1)
    void testGetOffsets() throws Exception {
        log.info("TEST: Get offsets endpoint");

        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(getTransformerUrl() + "/api/stream/animal/offsets"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode(), "Should return 200 OK");

        JsonNode body = objectMapper.readTree(response.body());
        log.info("Offsets response: {}", body);

        assertEquals("animal-transformer", body.get("consumerGroup").asText());
        assertEquals(CDC_TOPIC, body.get("topic").asText());
        assertNotNull(body.get("partitionOffsets"));
        assertTrue(body.get("partitionOffsets").isArray());
        assertNotNull(body.get("timestamp"));

        log.info("Get offsets test passed");
    }

    @Test
    @Order(2)
    void testGetOffsetsWithData() throws Exception {
        log.info("TEST: Get offsets with data");

        // Create an animal to generate CDC events
        long animalId = createAnimal("OffsetTestDog", "Beagle");
        log.info("Created animal with ID: {}", animalId);

        // Wait for it to be processed
        waitForAnimalInOutputTopic(animalId, 60);

        // Check offsets again
        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(getTransformerUrl() + "/api/stream/animal/offsets"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());

        JsonNode body = objectMapper.readTree(response.body());
        log.info("Offsets response after data: {}", body);

        // Should have offset info
        JsonNode partitionOffsets = body.get("partitionOffsets");
        assertTrue(partitionOffsets.isArray() && partitionOffsets.size() > 0);

        // End offset should be > 0 now
        long endOffset = partitionOffsets.get(0).get("endOffset").asLong();
        assertTrue(endOffset > 0, "End offset should be > 0 after creating data");

        log.info("Get offsets with data test passed");
    }

    @Test
    @Order(3)
    void testGetStatus() throws Exception {
        log.info("TEST: Get stream status");

        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(getTransformerUrl() + "/api/stream/animal/status"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode(), "Should return 200 OK");

        JsonNode body = objectMapper.readTree(response.body());
        log.info("Status response: {}", body);

        assertEquals("animal-transformer", body.get("applicationId").asText());
        assertTrue(body.get("running").asBoolean(), "Stream should be running");
        assertNotNull(body.get("state"));
        assertNotNull(body.get("timestamp"));

        log.info("Get status test passed");
    }

    @Test
    @Order(4)
    void testStopAndStartStream() throws Exception {
        log.info("TEST: Stop and start stream");

        // Stop the stream
        HttpResponse<String> stopResponse = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(getTransformerUrl() + "/api/stream/animal/stop"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        log.info("Stop response: {} - {}", stopResponse.statusCode(), stopResponse.body());
        assertEquals(200, stopResponse.statusCode(), "Stop endpoint should return 200");

        JsonNode stopBody = objectMapper.readTree(stopResponse.body());
        assertEquals("animal-transformer", stopBody.get("applicationId").asText());
        assertFalse(stopBody.get("running").asBoolean(), "Stream should not be running after stop");

        // Verify status shows not running
        HttpResponse<String> statusResponse = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(getTransformerUrl() + "/api/stream/animal/status"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        JsonNode statusBody = objectMapper.readTree(statusResponse.body());
        assertFalse(statusBody.get("running").asBoolean(), "Status should show stream not running");

        // Start the stream
        HttpResponse<String> startResponse = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(getTransformerUrl() + "/api/stream/animal/start"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        log.info("Start response: {} - {}", startResponse.statusCode(), startResponse.body());
        assertEquals(200, startResponse.statusCode(), "Start endpoint should return 200");

        JsonNode startBody = objectMapper.readTree(startResponse.body());
        assertEquals("animal-transformer", startBody.get("applicationId").asText());
        assertTrue(startBody.get("running").asBoolean(), "Stream should be running after start");

        // Wait for stream to stabilize
        Thread.sleep(5000);

        log.info("Stop and start stream test passed");
    }

    @Test
    @Order(5)
    void testResetOffsetsToEarliest() throws Exception {
        log.info("TEST: Reset offsets to earliest");

        // Create another animal to have some offset to reset
        long animalId = createAnimal("ResetTestDog", "Poodle");
        waitForAnimalInOutputTopic(animalId, 60);

        // Reset to earliest
        HttpResponse<String> resetResponse = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(getTransformerUrl() + "/api/stream/animal/offsets/reset/earliest"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        log.info("Reset to earliest response: {} - {}", resetResponse.statusCode(), resetResponse.body());
        assertEquals(200, resetResponse.statusCode(), "Reset to earliest should return 200");

        JsonNode body = objectMapper.readTree(resetResponse.body());
        assertTrue(body.get("success").asBoolean(), "Reset should be successful");
        assertEquals("Offsets reset successfully to earliest", body.get("message").asText());
        assertNotNull(body.get("previousOffsets"));
        assertNotNull(body.get("newOffsets"));

        // Wait for stream to stabilize after restart
        Thread.sleep(5000);

        log.info("Reset offsets to earliest test passed");
    }

    @Test
    @Order(6)
    void testResetOffsetsToLatest() throws Exception {
        log.info("TEST: Reset offsets to latest");

        HttpResponse<String> resetResponse = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(getTransformerUrl() + "/api/stream/animal/offsets/reset/latest"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        log.info("Reset to latest response: {} - {}", resetResponse.statusCode(), resetResponse.body());
        assertEquals(200, resetResponse.statusCode(), "Reset to latest should return 200");

        JsonNode body = objectMapper.readTree(resetResponse.body());
        assertTrue(body.get("success").asBoolean(), "Reset should be successful");
        assertEquals("Offsets reset successfully to latest", body.get("message").asText());

        // Wait for stream to stabilize after restart
        Thread.sleep(5000);

        log.info("Reset offsets to latest test passed");
    }

    @Test
    @Order(7)
    void testResetOffsetsToSpecific() throws Exception {
        log.info("TEST: Reset offsets to specific values");

        HttpResponse<String> resetResponse = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(getTransformerUrl() + "/api/stream/animal/offsets/reset"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"partitionOffsets\": {\"0\": 0}}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        log.info("Reset to specific response: {} - {}", resetResponse.statusCode(), resetResponse.body());
        assertEquals(200, resetResponse.statusCode(), "Reset to specific should return 200");

        JsonNode body = objectMapper.readTree(resetResponse.body());
        assertTrue(body.get("success").asBoolean(), "Reset should be successful");
        assertEquals("Offsets reset successfully to specified values", body.get("message").asText());

        // Wait for stream to stabilize after restart
        Thread.sleep(5000);

        log.info("Reset offsets to specific test passed");
    }

    @Test
    @Order(8)
    void testStreamStillProcessesAfterReset() throws Exception {
        log.info("TEST: Stream still processes after reset");

        // Create a new animal and verify it gets processed
        long animalId = createAnimal("PostResetDog", "Chihuahua");
        waitForAnimalInOutputTopic(animalId, 60);

        log.info("Stream still processes after reset test passed");
    }

    // Helper methods

    private static String getKafkaConnectUrl() {
        return "http://" + kafkaConnect.getHost() + ":" + kafkaConnect.getMappedPort(KAFKA_CONNECT_PORT);
    }

    private static String getAppUrl() {
        return "http://" + app.getHost() + ":" + app.getMappedPort(8080);
    }

    private static String getTransformerUrl() {
        return "http://" + transformer.getHost() + ":" + transformer.getMappedPort(8081);
    }

    private static String getSchemaRegistryUrlExternal() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT);
    }

    private static String getKafkaBootstrapServersExternal() {
        return "localhost:" + kafka.getMappedPort(KAFKA_EXTERNAL_PORT);
    }

    private static void registerDebeziumConnector() throws Exception {
        postgres.execInContainer("psql", "-U", "postgres", "-d", "eventdb", "-c",
                "CREATE PUBLICATION animal_publication FOR TABLE public.animal;");

        String connectorConfig = """
                {
                  "name": "animal-connector",
                  "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "database.hostname": "postgres", "database.port": "5432", "database.user": "postgres",
                    "database.password": "postgres", "database.dbname": "eventdb",
                    "topic.prefix": "dbserver1", "table.include.list": "public.animal",
                    "plugin.name": "pgoutput", "slot.name": "animal_slot_control", "publication.name": "animal_publication"
                  }
                }""";

        HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create(getKafkaConnectUrl() + "/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(connectorConfig)).build(),
                HttpResponse.BodyHandlers.ofString());
        log.info("Debezium connector registration response: {} - {}", response.statusCode(), response.body());
        assertTrue(response.statusCode() == 201 || response.statusCode() == 409, "Failed to register connector");
    }

    private static void waitForConnectorRunning() {
        await().atMost(90, TimeUnit.SECONDS).pollInterval(3, TimeUnit.SECONDS).until(() -> {
            try {
                HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                        .uri(URI.create(getKafkaConnectUrl() + "/connectors/animal-connector/status")).build(),
                        HttpResponse.BodyHandlers.ofString());
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

    private long createAnimal(String name, String breed) throws IOException, InterruptedException {
        String body = String.format("{\"name\": \"%s\", \"breed\": \"%s\"}", name, breed);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getAppUrl() + "/api/animals"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        long id = objectMapper.readTree(response.body()).get("id").asLong();
        log.info("Created animal '{}' with ID: {}", name, id);
        return id;
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

    private void waitForAnimalInOutputTopic(long animalId, int timeoutSeconds) {
        await().atMost(timeoutSeconds, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            return isAnimalInOutputTopic(animalId);
        });
    }

    private boolean isAnimalInOutputTopic(long animalId) {
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = createAvroConsumer(ANIMAL_DETAILS_TOPIC)) {
            List<ConsumerRecord<GenericRecord, GenericRecord>> allRecords = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    allRecords.add(record);
                }
            }

            for (ConsumerRecord<GenericRecord, GenericRecord> record : allRecords) {
                GenericRecord key = record.key();
                if (key != null && key.getSchema().getName().equals("RecordKey")) {
                    long id = ((Number) key.get("id")).longValue();
                    if (id == animalId) {
                        log.info("Found animal {} in output topic", animalId);
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
