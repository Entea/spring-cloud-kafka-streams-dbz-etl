package com.example.integration.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Helper class for managing Debezium connectors in integration tests.
 */
public class DebeziumConnectorHelper {

    private static final Logger log = LoggerFactory.getLogger(DebeziumConnectorHelper.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private final String kafkaConnectUrl;

    public DebeziumConnectorHelper(String kafkaConnectUrl) {
        this.kafkaConnectUrl = kafkaConnectUrl;
    }

    /**
     * Wait for a connector to be in RUNNING state.
     */
    public void waitForConnectorRunning(String connectorName) {
        waitForConnectorRunning(connectorName, 90);
    }

    /**
     * Wait for a connector to be in RUNNING state with custom timeout.
     */
    public void waitForConnectorRunning(String connectorName, int timeoutSeconds) {
        await().atMost(timeoutSeconds, TimeUnit.SECONDS)
                .pollInterval(3, TimeUnit.SECONDS)
                .until(() -> isConnectorRunning(connectorName));
        log.info("Debezium connector '{}' and task are running", connectorName);
    }

    /**
     * Check if a connector is in RUNNING state.
     */
    public boolean isConnectorRunning(String connectorName) {
        try {
            HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                    .uri(URI.create(kafkaConnectUrl + "/connectors/" + connectorName + "/status"))
                    .build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                var status = objectMapper.readTree(response.body());
                String connectorState = status.path("connector").path("state").asText();
                var tasks = status.path("tasks");
                log.debug("Connector '{}' state: {}, tasks: {}", connectorName, connectorState, tasks);

                if ("RUNNING".equals(connectorState) && !tasks.isEmpty() &&
                        "RUNNING".equals(tasks.get(0).path("state").asText())) {
                    return true;
                }
            }
        } catch (Exception e) {
            log.debug("Waiting for connector '{}': {}", connectorName, e.getMessage());
        }
        return false;
    }

}
