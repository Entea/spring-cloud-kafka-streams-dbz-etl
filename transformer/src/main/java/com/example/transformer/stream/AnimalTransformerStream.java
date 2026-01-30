package com.example.transformer.stream;

import com.example.transformer.avro.AnimalDetails;
import com.example.transformer.avro.RecordKey;
import com.example.transformer.service.AnimalEnrichmentService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Function;

@Configuration
public class AnimalTransformerStream {

    private static final Logger logger = LoggerFactory.getLogger(AnimalTransformerStream.class);
    private static final String DLQ_TOPIC = "animal-transformer-dlq";

    private final AnimalEnrichmentService enrichmentService;

    public AnimalTransformerStream(AnimalEnrichmentService enrichmentService) {
        this.enrichmentService = enrichmentService;
    }

    /**
     * Wrapper for enrichment result to enable branching
     */
    private record EnrichResult(GenericRecord original, AnimalDetails enriched, boolean failed) {}

    @Bean
    public Function<KStream<GenericRecord, GenericRecord>, KStream<RecordKey, AnimalDetails>> animalTransform() {
        return stream -> {
            // Try enrichment and capture result
            KStream<GenericRecord, EnrichResult> processed = stream
                    .filter((key, value) -> value != null)
                    .mapValues(value -> {
                        try {
                            AnimalDetails enriched = enrichmentService.extractAndEnrich(value);
                            return new EnrichResult(value, enriched, false);
                        } catch (Exception e) {
                            logger.error("Failed to enrich animal record", e);
                            return new EnrichResult(value, null, true);
                        }
                    });

            // Split into success and failure branches
            KStream<GenericRecord, EnrichResult>[] branches = processed
                    .branch(
                            (k, v) -> v.failed(),     // Branch 0: failures -> DLQ
                            (k, v) -> !v.failed()     // Branch 1: successes -> continue
                    );

            // Branch 0: Send failures to DLQ (keep original CDC message)
            branches[0]
                    .mapValues(EnrichResult::original)
                    .peek((k, v) -> logger.info("Sending failed record to DLQ"))
                    .to(DLQ_TOPIC);

            // Branch 1: Process successes
            return branches[1]
                    .mapValues(EnrichResult::enriched)
                    .filter((key, value) -> value != null)
                    .selectKey((key, value) -> RecordKey.newBuilder().setId(value.getId()).build());
        };
    }
}
