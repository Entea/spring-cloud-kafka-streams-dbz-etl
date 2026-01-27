package com.example.transformer.stream;

import com.example.transformer.avro.AnimalDetails;
import com.example.transformer.service.AnimalEnrichmentService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.function.Function;

@Configuration
public class AnimalDlqTransformerStream {

    private final AnimalEnrichmentService enrichmentService;

    public AnimalDlqTransformerStream(AnimalEnrichmentService enrichmentService) {
        this.enrichmentService = enrichmentService;
    }

    @Bean
    public Function<KStream<String, GenericRecord>, KStream<String, AnimalDetails>> animalDlqTransform() {
        return stream -> stream
                .filter((key, value) -> value != null)
                .mapValues(enrichmentService::extractAndEnrich)
                .filter((key, value) -> value != null);
    }

    @RestController
    @RequestMapping("/api/dlq/animal")
    public static class AnimalDlqStreamControl {

        private static final Logger logger = LoggerFactory.getLogger(AnimalDlqStreamControl.class);

        private final BindingsLifecycleController bindingsController;

        public AnimalDlqStreamControl(BindingsLifecycleController bindingsController) {
            this.bindingsController = bindingsController;
        }

        @PostMapping("/start")
        public ResponseEntity<String> startDlqStream() {
            logger.info("Starting Animal DLQ stream");
            bindingsController.changeState("animalDlqTransform-in-0", State.STARTED);
            bindingsController.changeState("animalDlqTransform-out-0", State.STARTED);
            return ResponseEntity.ok("Animal DLQ stream started");
        }

        @PostMapping("/stop")
        public ResponseEntity<String> stopDlqStream() {
            logger.info("Stopping Animal DLQ stream");
            bindingsController.changeState("animalDlqTransform-in-0", State.STOPPED);
            bindingsController.changeState("animalDlqTransform-out-0", State.STOPPED);
            return ResponseEntity.ok("Animal DLQ stream stopped");
        }
    }
}
