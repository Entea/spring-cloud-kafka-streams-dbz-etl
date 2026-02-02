package com.example.integration;

import com.example.integration.utils.HttpTestHelper;
import com.example.integration.utils.KafkaTestHelper;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the CDC pipeline using Event entity.
 * Tests that events created/updated via REST API flow through Debezium CDC
 * to the event-details Kafka topic.
 */
public class CdcPipelineIT extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(CdcPipelineIT.class);

    private static final String EVENT_DETAILS_TOPIC = "event-details";

    private static KafkaTestHelper kafkaHelper;
    private static HttpTestHelper httpHelper;

    @BeforeAll
    static void setUp() throws Exception {
        startEnvironment();

        kafkaHelper = new KafkaTestHelper(getKafkaBootstrapServers(), getSchemaRegistryUrl());
        httpHelper = new HttpTestHelper(getAppUrl(), getTransformerUrl());

        listKafkaTopics();
    }

    @Test
    void testCreateAndUpdateEventAppearsInKafkaTopic() throws Exception {
        List<GenericRecord> receivedEvents = Collections.synchronizedList(new ArrayList<>());

        // Start consuming in a background thread
        Thread consumerThread = kafkaHelper.startBackgroundConsumer(EVENT_DETAILS_TOPIC, receivedEvents);

        try {
            // Create an event via REST API
            log.info("Creating event via REST API at {}", getAppUrl());
            var createdEvent = httpHelper.createEvent("test-event-1");
            assertNotNull(createdEvent);
            long eventId = createdEvent.get("id").asLong();
            log.info("Created event with ID: {}", eventId);

            // Wait for the created event to appear in Kafka
            await().atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(() -> receivedEvents.stream()
                            .anyMatch(e -> ((Number) e.get("id")).longValue() == eventId
                                    && "test-event-1".equals(e.get("name").toString())));

            log.info("Create event appeared in Kafka topic");

            // Update the event via REST API
            var updatedEvent = httpHelper.updateEvent(eventId, "test-event-1-updated");
            assertNotNull(updatedEvent);
            assertEquals("test-event-1-updated", updatedEvent.get("name").asText());
            log.info("Updated event: {}", updatedEvent);

            // Wait for the updated event to appear in Kafka
            await().atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(() -> receivedEvents.stream()
                            .anyMatch(e -> ((Number) e.get("id")).longValue() == eventId
                                    && "test-event-1-updated".equals(e.get("name").toString())));

            log.info("Update event appeared in Kafka topic");
        } finally {
            // Stop consumer thread
            consumerThread.interrupt();
            consumerThread.join(5000);
        }

        // Verify we received both events
        assertTrue(receivedEvents.stream()
                        .anyMatch(e -> "test-event-1".equals(e.get("name").toString())),
                "Should have received the created event");
        assertTrue(receivedEvents.stream()
                        .anyMatch(e -> "test-event-1-updated".equals(e.get("name").toString())),
                "Should have received the updated event");

        log.info("Test completed successfully! Received {} events total", receivedEvents.size());
    }
}
