package com.example.transformer.controller;

import com.example.transformer.service.AnimalEnrichmentService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.PreDestroy;
import java.util.Map;

@RestController
@RequestMapping("/api/export/animal")
public class ManualAnimalExportController {

    private static final Logger logger = LoggerFactory.getLogger(ManualAnimalExportController.class);

    private static final String OUTPUT_TOPIC = "animal-details";

    private final AnimalEnrichmentService enrichmentService;
    private final KafkaProducer<String, String> producer;

    public ManualAnimalExportController(
            AnimalEnrichmentService enrichmentService,
            @Value("${spring.cloud.stream.kafka.streams.binder.brokers}") String brokers) {
        this.enrichmentService = enrichmentService;
        this.producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));
    }

    @PostMapping
    public ResponseEntity<String> export(@RequestBody String cdcPayload) {
        logger.info("Manual animal export requested");

        String enriched = enrichmentService.extractAndEnrich(cdcPayload);
        if (enriched == null) {
            return ResponseEntity.unprocessableEntity().body("Could not enrich payload");
        }

        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, enriched));
        producer.flush();
        logger.info("Manual animal export sent to {}", OUTPUT_TOPIC);
        return ResponseEntity.ok(enriched);
    }

    @PreDestroy
    public void close() {
        producer.close();
    }
}
