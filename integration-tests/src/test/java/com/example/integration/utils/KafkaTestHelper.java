package com.example.integration.utils;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Helper class for Kafka consumer operations in integration tests.
 */
public class KafkaTestHelper {

    private static final Logger log = LoggerFactory.getLogger(KafkaTestHelper.class);

    private final String bootstrapServers;
    private final String schemaRegistryUrl;

    public KafkaTestHelper(String bootstrapServers, String schemaRegistryUrl) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    /**
     * Create an Avro consumer for the specified topic.
     */
    public KafkaConsumer<GenericRecord, GenericRecord> createAvroConsumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-consumer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    /**
     * Start a background consumer thread that collects records into the provided list.
     * Returns the thread so it can be interrupted and joined later.
     */
    public Thread startBackgroundConsumer(String topic, List<GenericRecord> receivedRecords) {
        Thread consumerThread = new Thread(() -> {
            try (KafkaConsumer<GenericRecord, GenericRecord> consumer = createAvroConsumer(topic)) {
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                        try {
                            log.info("Received record: {}", record.value());
                            receivedRecords.add(record.value());
                        } catch (Exception e) {
                            log.error("Failed to process record", e);
                        }
                    }
                }
            }
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
        return consumerThread;
    }

    /**
     * Check if a record key matches the expected ID (for RecordKey schema).
     */
    public static boolean keyMatchesId(GenericRecord key, long expectedId) {
        if (key != null && key.getSchema().getName().equals("RecordKey")) {
            return ((Number) key.get("id")).longValue() == expectedId;
        }
        return false;
    }

    /**
     * Check if a CDC record's "after" field matches the expected ID.
     */
    public static boolean cdcRecordMatchesId(GenericRecord value, long expectedId) {
        if (value == null) {
            return false;
        }
        // Check if the schema has an "after" field (CDC format)
        if (value.getSchema().getField("after") != null) {
            Object after = value.get("after");
            if (after instanceof GenericRecord afterRecord) {
                return ((Number) afterRecord.get("id")).longValue() == expectedId;
            }
        }
        return false;
    }

    /**
     * Check if a record matches the expected ID (checks both key and CDC value formats).
     */
    public static boolean recordMatchesId(ConsumerRecord<GenericRecord, GenericRecord> record, long expectedId) {
        // Check if the key is the enriched key from the final topic
        if (keyMatchesId(record.key(), expectedId)) {
            return true;
        }
        // Check if the value is the original Debezium message
        return cdcRecordMatchesId(record.value(), expectedId);
    }
}
