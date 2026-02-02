package com.example.integration;

import com.example.integration.utils.HttpTestHelper;
import com.example.integration.utils.KafkaTestHelper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration test for DLQ (Dead Letter Queue) processing pipeline.
 *
 * <p>Uses docker-compose.dlq.yml override to configure a broken APP_SERVICE_URL
 * that causes transformer enrichment to fail, triggering DLQ routing.</p>
 */
public class DlqPipelineIT extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(DlqPipelineIT.class);

    private static final String ANIMAL_DETAILS_TOPIC = "animal-details";
    private static final String DLQ_TOPIC = "animal-transformer-dlq";
    private static final String CDC_TOPIC = "dbserver1.public.animal";

    private static KafkaTestHelper kafkaHelper;
    private static HttpTestHelper httpHelper;

    @BeforeAll
    static void setUp() throws Exception {
        // Start with test and DLQ overrides to configure broken APP_SERVICE_URL
        startEnvironment(
                new File("../docker-compose.yml"),
                new File("../docker-compose.dlq.yml")
        );

        kafkaHelper = new KafkaTestHelper(getKafkaBootstrapServers(), getSchemaRegistryUrl());
        httpHelper = new HttpTestHelper(getAppUrl(), getTransformerUrl());

        listKafkaTopics();
    }

    @Test
    void testAnimalDlqProcessing() throws Exception {
        log.info("PHASE 1: Triggering DLQ...");
        long failedAnimalId = httpHelper.createAnimal("FailDog", "Unknown");

        // First, verify CDC topic received the message
        log.info("Checking CDC topic {} for animal {}", CDC_TOPIC, failedAnimalId);
        try (KafkaConsumer<GenericRecord, GenericRecord> cdcConsumer = kafkaHelper.createAvroConsumer(CDC_TOPIC)) {
            await().atMost(60, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(() -> {
                ConsumerRecords<GenericRecord, GenericRecord> records = cdcConsumer.poll(Duration.ofMillis(1000));
                log.info("CDC topic poll returned {} records", records.count());
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    log.info("CDC record key: {}, value schema: {}", record.key(),
                            record.value() != null ? record.value().getSchema().getName() : "null");
                    if (KafkaTestHelper.cdcRecordMatchesId(record.value(), failedAnimalId)) {
                        log.info("Found CDC record for animal ID: {}", failedAnimalId);
                        return true;
                    }
                }
                return false;
            });
        }
        log.info("CDC message confirmed for animal {}", failedAnimalId);

        // Check what topics exist now
        listKafkaTopics();

        // Also check animal-details topic to see if messages went there instead
        log.info("Checking animal-details topic for any messages");
        try (KafkaConsumer<GenericRecord, GenericRecord> detailsConsumer = kafkaHelper.createAvroConsumer(ANIMAL_DETAILS_TOPIC)) {
            ConsumerRecords<GenericRecord, GenericRecord> detailsRecords = detailsConsumer.poll(Duration.ofSeconds(5));
            log.info("animal-details topic has {} records", detailsRecords.count());
            for (ConsumerRecord<GenericRecord, GenericRecord> record : detailsRecords) {
                log.info("animal-details record: {}", record.value());
            }
        }

        // Print transformer container logs for debugging
        log.info("Transformer container logs (last 50 lines):");
        String logs = getTransformerLogs();
        String[] logLines = logs.split("\n");
        int startIdx = Math.max(0, logLines.length - 50);
        for (int i = startIdx; i < logLines.length; i++) {
            log.info("  TRANSFORMER: {}", logLines[i]);
        }

        // Now check DLQ topic
        log.info("Checking DLQ topic {} for animal {}", DLQ_TOPIC, failedAnimalId);
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = kafkaHelper.createAvroConsumer(DLQ_TOPIC)) {
            await().atMost(60, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(() -> {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                log.info("DLQ topic poll returned {} records", records.count());
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    log.info("DLQ record: key={}, value schema={}", record.key(),
                            record.value() != null ? record.value().getSchema().getName() : "null");
                    if (KafkaTestHelper.recordMatchesId(record, failedAnimalId)) {
                        log.info("Verified message for animal {} in DLQ topic.", failedAnimalId);
                        return true;
                    }
                }
                return false;
            });
        }
        log.info("DLQ message confirmed for animal {}", failedAnimalId);

        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = kafkaHelper.createAvroConsumer(ANIMAL_DETAILS_TOPIC)) {
            ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                if (KafkaTestHelper.recordMatchesId(record, failedAnimalId)) {
                    fail("Message for animal " + failedAnimalId + " should not be in final topic yet.");
                }
            }
            log.info("Verified message for animal {} is NOT in final topic.", failedAnimalId);
        }

        log.info("PHASE 2: Starting DLQ reprocessing...");
        httpHelper.controlDlqStream("start");

        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = kafkaHelper.createAvroConsumer(ANIMAL_DETAILS_TOPIC)) {
            await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    if (KafkaTestHelper.recordMatchesId(record, failedAnimalId)) {
                        assertEquals("FailDog", record.value().get("name").toString());
                        log.info("Verified reprocessed message for animal {} in final topic.", failedAnimalId);
                        return true;
                    }
                }
                return false;
            });
        }

        log.info("PHASE 3: Stopping DLQ stream and verifying...");
        httpHelper.controlDlqStream("stop");
        long anotherFailedId = httpHelper.createAnimal("StopTestDog", "Poodle");

        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = kafkaHelper.createAvroConsumer(DLQ_TOPIC)) {
            await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    if (KafkaTestHelper.recordMatchesId(record, anotherFailedId)) {
                        log.info("Verified second message for animal {} in DLQ topic.", anotherFailedId);
                        return true;
                    }
                }
                return false;
            });
        }

        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = kafkaHelper.createAvroConsumer(ANIMAL_DETAILS_TOPIC)) {
            ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                if (KafkaTestHelper.recordMatchesId(record, anotherFailedId)) {
                    fail("Message for " + anotherFailedId + " should not be processed after stopping DLQ stream.");
                }
            }
            log.info("Verified second message for animal {} is NOT in final topic.", anotherFailedId);
        }
    }
}
