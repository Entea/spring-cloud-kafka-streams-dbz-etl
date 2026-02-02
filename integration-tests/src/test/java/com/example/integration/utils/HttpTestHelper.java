package com.example.integration.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Helper class for HTTP operations in integration tests.
 */
public class HttpTestHelper {

    private static final Logger log = LoggerFactory.getLogger(HttpTestHelper.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private final String appUrl;
    private final String transformerUrl;

    public HttpTestHelper(String appUrl, String transformerUrl) {
        this.appUrl = appUrl;
        this.transformerUrl = transformerUrl;
    }

    /**
     * Create an event via REST API.
     */
    public JsonNode createEvent(String name) throws IOException, InterruptedException {
        HttpResponse<String> response = postJson(appUrl + "/api/events", String.format("{\"name\": \"%s\"}", name));
        assertEquals(200, response.statusCode(), "Failed to create event: " + response.body());
        JsonNode json = objectMapper.readTree(response.body());
        log.info("Created event '{}' with ID: {}", name, json.get("id").asLong());
        return json;
    }

    /**
     * Update an event via REST API.
     */
    public JsonNode updateEvent(long id, String newName) throws IOException, InterruptedException {
        HttpResponse<String> response = putJson(appUrl + "/api/events/" + id, String.format("{\"name\": \"%s\"}", newName));
        assertEquals(200, response.statusCode(), "Failed to update event: " + response.body());
        JsonNode json = objectMapper.readTree(response.body());
        log.info("Updated event {} to name '{}'", id, newName);
        return json;
    }

    /**
     * Create an animal via REST API.
     */
    public long createAnimal(String name, String breed) throws IOException, InterruptedException {
        String body = String.format("{\"name\": \"%s\", \"breed\": \"%s\"}", name, breed);
        HttpResponse<String> response = postJson(appUrl + "/api/animals", body);

        assertEquals(200, response.statusCode(), "Failed to create animal: " + response.body());
        long id = objectMapper.readTree(response.body()).get("id").asLong();
        log.info("Created animal '{}' ({}) with ID: {}", name, breed, id);
        return id;
    }

    /**
     * Get animal stream offsets from transformer.
     */
    public JsonNode getAnimalStreamOffsets() throws IOException, InterruptedException {
        HttpResponse<String> response = get(transformerUrl + "/api/stream/animal/offsets");
        assertEquals(200, response.statusCode(), "Failed to get offsets: " + response.body());
        return objectMapper.readTree(response.body());
    }

    /**
     * Get animal stream status from transformer.
     */
    public JsonNode getAnimalStreamStatus() throws IOException, InterruptedException {
        HttpResponse<String> response = get(transformerUrl + "/api/stream/animal/status");
        assertEquals(200, response.statusCode(), "Failed to get status: " + response.body());
        return objectMapper.readTree(response.body());
    }

    /**
     * Stop the animal stream.
     */
    public JsonNode stopAnimalStream() throws IOException, InterruptedException {
        HttpResponse<String> response = postNoBody(transformerUrl + "/api/stream/animal/stop");
        assertEquals(200, response.statusCode(), "Failed to stop stream: " + response.body());
        log.info("Stopped animal stream");
        return objectMapper.readTree(response.body());
    }

    /**
     * Start the animal stream.
     */
    public JsonNode startAnimalStream() throws IOException, InterruptedException {
        HttpResponse<String> response = postNoBody(transformerUrl + "/api/stream/animal/start");
        assertEquals(200, response.statusCode(), "Failed to start stream: " + response.body());
        log.info("Started animal stream");
        return objectMapper.readTree(response.body());
    }

    /**
     * Reset animal stream offsets to earliest.
     */
    public JsonNode resetAnimalStreamToEarliest() throws IOException, InterruptedException {
        HttpResponse<String> response = postNoBody(transformerUrl + "/api/stream/animal/offsets/reset/earliest");
        assertEquals(200, response.statusCode(), "Failed to reset offsets: " + response.body());
        log.info("Reset animal stream offsets to earliest");
        return objectMapper.readTree(response.body());
    }

    /**
     * Reset animal stream offsets to latest.
     */
    public JsonNode resetAnimalStreamToLatest() throws IOException, InterruptedException {
        HttpResponse<String> response = postNoBody(transformerUrl + "/api/stream/animal/offsets/reset/latest");
        assertEquals(200, response.statusCode(), "Failed to reset offsets: " + response.body());
        log.info("Reset animal stream offsets to latest");
        return objectMapper.readTree(response.body());
    }

    /**
     * Reset animal stream offsets to specific values.
     */
    public JsonNode resetAnimalStreamToSpecific(String partitionOffsetsJson) throws IOException, InterruptedException {
        HttpResponse<String> response = postJson(transformerUrl + "/api/stream/animal/offsets/reset", "{\"partitionOffsets\": " + partitionOffsetsJson + "}");
        assertEquals(200, response.statusCode(), "Failed to reset offsets: " + response.body());
        log.info("Reset animal stream offsets to specific values");
        return objectMapper.readTree(response.body());
    }

    /**
     * Control DLQ stream (start/stop).
     */
    public void controlDlqStream(String action) throws IOException, InterruptedException {
        HttpResponse<String> response = postNoBody(transformerUrl + "/api/dlq/animal/" + action);
        assertEquals(200, response.statusCode(), "Failed to " + action + " DLQ stream: " + response.body());
        log.info("Sent '{}' command to DLQ stream. Response: {}", action, response.body());
    }

    /**
     * Generic GET request.
     */
    public HttpResponse<String> get(String url) throws IOException, InterruptedException {
        return httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Generic POST request with JSON body.
     */
    public HttpResponse<String> postJson(String url, String body) throws IOException, InterruptedException {
        return httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postNoBody(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> putJson(String url, String body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
