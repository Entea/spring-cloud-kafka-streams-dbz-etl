package com.example.transformer.controller;

import com.example.transformer.avro.AnimalDetails;
import com.example.transformer.avro.RecordKey;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/export/animal")
public class ManualAnimalExportController {

    private static final Logger logger = LoggerFactory.getLogger(ManualAnimalExportController.class);

    private static final String OUTPUT_TOPIC = "animal-details";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaProducer<RecordKey, AnimalDetails> producer;

    public ManualAnimalExportController(
            @Value("${spring.cloud.stream.kafka.streams.binder.brokers}") String brokers,
            @Value("${spring.cloud.stream.kafka.streams.binder.configuration.schema.registry.url}") String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        config.put("schema.registry.url", schemaRegistryUrl);
        this.producer = new KafkaProducer<>(config);
    }

    @PostMapping
    public ResponseEntity<String> export(@RequestBody String jsonPayload) {
        logger.info("Manual animal export requested");

        try {
            JsonNode node = objectMapper.readTree(jsonPayload);

            JsonNode after = node.path("after");
            if (after.isMissingNode() || after.isNull()) {
                after = node;
            }

            AnimalDetails enriched = AnimalDetails.newBuilder()
                    .setId(after.path("id").asLong())
                    .setVersion(after.path("version").asLong())
                    .setName(after.path("name").asText(""))
                    .setBreed(after.path("breed").asText(""))
                    .build();

            RecordKey key = RecordKey.newBuilder()
                    .setId(enriched.getId())
                    .build();

            producer.send(new ProducerRecord<>(OUTPUT_TOPIC, key, enriched));
            producer.flush();
            logger.info("Manual animal export sent to {}", OUTPUT_TOPIC);
            return ResponseEntity.ok(enriched.toString());
        } catch (Exception e) {
            logger.error("Failed to process manual export: {}", e.getMessage(), e);
            return ResponseEntity.unprocessableEntity().body("Could not enrich payload: " + e.getMessage());
        }
    }

    @PreDestroy
    public void close() {
        producer.close();
    }
}
