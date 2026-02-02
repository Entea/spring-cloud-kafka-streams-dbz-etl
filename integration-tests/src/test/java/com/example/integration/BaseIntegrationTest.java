package com.example.integration;

import com.example.integration.utils.DebeziumConnectorHelper;
import org.junit.jupiter.api.AfterAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Base class for integration tests using ComposeContainer.
 *
 * <p>Uses the project's docker-compose.yml to start all services with proper
 * dependency ordering and healthchecks. Requires images to be pre-built via
 * {@code ./build-images.sh} before running tests.</p>
 *
 * <p>Subclasses should call {@link #startEnvironment(File...)} from their
 * {@code @BeforeAll} method to start the environment with the desired
 * compose files.</p>
 */
public abstract class BaseIntegrationTest {

    protected static final Logger log = LoggerFactory.getLogger(BaseIntegrationTest.class);

    protected static final String KAFKA_SERVICE = "kafka";
    protected static final String TRANSFORMER_SERVICE = "transformer";

    // Ports as defined in docker-compose.yml and docker-compose.test.yml
    protected static final int KAFKA_EXTERNAL_PORT = 29092;
    protected static final int SCHEMA_REGISTRY_EXTERNAL_PORT = 8086;
    protected static final int KAFKA_CONNECT_EXTERNAL_PORT = 8084;
    protected static final int APP_EXTERNAL_PORT = 8082;
    protected static final int TRANSFORMER_EXTERNAL_PORT = 8081;

    protected static ComposeContainer environment;

    /**
     * Start the Docker Compose environment with the default docker-compose.yml and test overlay.
     */
    protected static void startEnvironment() throws Exception {
        startEnvironment(
                new File("../docker-compose.yml")
        );
    }

    /**
     * Start the Docker Compose environment with the specified compose files.
     * Multiple files are merged (like docker-compose -f file1 -f file2).
     */
    protected static void startEnvironment(File... composeFiles) throws Exception {
        environment = new ComposeContainer(composeFiles)
                .withLocalCompose(true);

        log.info("Starting Docker Compose environment with {} file(s)...", composeFiles.length);
        environment.start();
        log.info("Docker Compose environment started successfully");

        // Wait for all services to be reachable
        waitForServices();

        // Wait for transformer to fully initialize Kafka Streams
        log.info("Waiting for transformer to initialize Kafka Streams...");
        Thread.sleep(10000);
    }

    /**
     * Wait for all services to be reachable via their health endpoints.
     */
    private static void waitForServices() {
        log.info("Waiting for services to be reachable...");

        // Wait for schema registry
        await().atMost(120, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> {
                    try (HttpClient httpClient = HttpClient.newHttpClient()) {
                        var response = httpClient.send(
                                HttpRequest.newBuilder()
                                        .uri(URI.create(getSchemaRegistryUrl() + "/subjects"))
                                        .GET().build(),
                                HttpResponse.BodyHandlers.ofString());
                        return response.statusCode() == 200;
                    }
                });
        log.info("Schema Registry is ready");

        // Wait for Kafka Connect
        await().atMost(120, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> {
                    try (HttpClient httpClient = HttpClient.newHttpClient()) {
                        var response = httpClient.send(
                                HttpRequest.newBuilder()
                                        .uri(URI.create(getKafkaConnectUrl() + "/connectors"))
                                        .GET().build(),
                                HttpResponse.BodyHandlers.ofString());
                        return response.statusCode() == 200;
                    }
                });
        log.info("Kafka Connect is ready");

        // Wait for App
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> {
                    try (HttpClient httpClient = HttpClient.newHttpClient()) {
                        var response = httpClient.send(
                                HttpRequest.newBuilder()
                                        .uri(URI.create(getAppUrl() + "/api/events"))
                                        .GET().build(),
                                HttpResponse.BodyHandlers.ofString());
                        return response.statusCode() == 200;
                    }
                });
        log.info("App is ready");

        // Waiting for event-connector
        var connectorHelper = new DebeziumConnectorHelper(getKafkaConnectUrl());
        connectorHelper.waitForConnectorRunning("event-connector");

        log.info("All services are reachable");
    }

    @AfterAll
    static void stopEnvironment() {
        if (environment != null) {
            log.info("Stopping Docker Compose environment...");
            environment.stop();
            log.info("Docker Compose environment stopped");
        }
    }

    /**
     * Get the external URL for accessing Kafka from the test.
     * Uses the fixed port from docker-compose.test.yml.
     */
    protected static String getKafkaBootstrapServers() {
        return "localhost:" + KAFKA_EXTERNAL_PORT;
    }

    /**
     * Get the external URL for the Schema Registry.
     * Uses the fixed port from docker-compose.yml.
     */
    protected static String getSchemaRegistryUrl() {
        return "http://localhost:" + SCHEMA_REGISTRY_EXTERNAL_PORT;
    }

    /**
     * Get the external URL for Kafka Connect REST API.
     * Uses the fixed port from docker-compose.yml.
     */
    protected static String getKafkaConnectUrl() {
        return "http://localhost:" + KAFKA_CONNECT_EXTERNAL_PORT;
    }

    /**
     * Get the external URL for the App REST API.
     * Uses the fixed port from docker-compose.yml.
     */
    protected static String getAppUrl() {
        return "http://localhost:" + APP_EXTERNAL_PORT;
    }

    /**
     * Get the external URL for the Transformer service.
     * Uses the fixed port from docker-compose.yml.
     */
    protected static String getTransformerUrl() {
        return "http://localhost:" + TRANSFORMER_EXTERNAL_PORT;
    }

    /**
     * Get a container by service name for executing commands.
     */
    protected static Optional<ContainerState> getContainer(String serviceName) {
        return environment.getContainerByServiceName(serviceName + "-1");
    }

    /**
     * List Kafka topics for debugging.
     */
    protected static void listKafkaTopics() {
        try {
            Optional<ContainerState> kafka = getContainer(KAFKA_SERVICE);
            if (kafka.isPresent()) {
                var result = kafka.get().execInContainer("kafka-topics", "--bootstrap-server", "localhost:9092", "--list");
                log.info("Available Kafka topics: {}", result.getStdout());
            }
        } catch (Exception e) {
            log.warn("Could not list Kafka topics", e);
        }
    }

    /**
     * Get transformer container logs for debugging.
     */
    protected static String getTransformerLogs() {
        try {
            Optional<ContainerState> transformer = getContainer(TRANSFORMER_SERVICE);
            if (transformer.isPresent()) {
                return transformer.get().getLogs();
            }
        } catch (Exception e) {
            log.warn("Could not get transformer logs", e);
        }
        return "";
    }
}
