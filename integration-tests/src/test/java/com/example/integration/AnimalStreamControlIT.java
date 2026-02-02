package com.example.integration;

import com.example.integration.utils.HttpTestHelper;
import com.example.integration.utils.KafkaTestHelper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for animal stream control endpoints.
 * Tests start/stop stream, get status, and offset reset functionality.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AnimalStreamControlIT extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AnimalStreamControlIT.class);

    private static final String ANIMAL_DETAILS_TOPIC = "animal-details";
    private static final String CDC_TOPIC = "dbserver1.public.animal";

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
    @Order(1)
    void testGetOffsets() throws Exception {
        log.info("TEST: Get offsets endpoint");

        JsonNode body = httpHelper.getAnimalStreamOffsets();
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
        long animalId = httpHelper.createAnimal("OffsetTestDog", "Beagle");
        log.info("Created animal with ID: {}", animalId);

        // Wait for it to be processed
        waitForAnimalInOutputTopic(animalId, 60);

        // Check offsets again
        JsonNode body = httpHelper.getAnimalStreamOffsets();
        log.info("Offsets response after data: {}", body);

        // Should have offset info
        JsonNode partitionOffsets = body.get("partitionOffsets");
        assertTrue(partitionOffsets.isArray() && !partitionOffsets.isEmpty());

        // End offset should be > 0 now
        long endOffset = partitionOffsets.get(0).get("endOffset").asLong();
        assertTrue(endOffset > 0, "End offset should be > 0 after creating data");

        log.info("Get offsets with data test passed");
    }

    @Test
    @Order(3)
    void testGetStatus() throws Exception {
        log.info("TEST: Get stream status");

        JsonNode body = httpHelper.getAnimalStreamStatus();
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
        JsonNode stopBody = httpHelper.stopAnimalStream();
        assertEquals("animal-transformer", stopBody.get("applicationId").asText());
        assertFalse(stopBody.get("running").asBoolean(), "Stream should not be running after stop");

        // Verify status shows not running
        JsonNode statusBody = httpHelper.getAnimalStreamStatus();
        assertFalse(statusBody.get("running").asBoolean(), "Status should show stream not running");

        // Start the stream
        JsonNode startBody = httpHelper.startAnimalStream();
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
        long animalId = httpHelper.createAnimal("ResetTestDog", "Poodle");
        waitForAnimalInOutputTopic(animalId, 60);

        // Reset to earliest
        JsonNode body = httpHelper.resetAnimalStreamToEarliest();
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

        JsonNode body = httpHelper.resetAnimalStreamToLatest();
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

        JsonNode body = httpHelper.resetAnimalStreamToSpecific("{\"0\": 0}");
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
        long animalId = httpHelper.createAnimal("PostResetDog", "Chihuahua");
        waitForAnimalInOutputTopic(animalId, 60);

        log.info("Stream still processes after reset test passed");
    }

    private void waitForAnimalInOutputTopic(long animalId, int timeoutSeconds) {
        await().atMost(timeoutSeconds, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> isAnimalInOutputTopic(animalId));
    }

    private boolean isAnimalInOutputTopic(long animalId) {
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = kafkaHelper.createAvroConsumer(ANIMAL_DETAILS_TOPIC)) {
            List<ConsumerRecord<GenericRecord, GenericRecord>> allRecords = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    allRecords.add(record);
                }
            }

            for (ConsumerRecord<GenericRecord, GenericRecord> record : allRecords) {
                if (KafkaTestHelper.keyMatchesId(record.key(), animalId)) {
                    log.info("Found animal {} in output topic", animalId);
                    return true;
                }
            }
            return false;
        }
    }
}
