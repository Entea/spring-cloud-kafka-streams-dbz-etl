package com.example.transformer.stream;

import com.example.transformer.avro.AnimalDetails;
import com.example.transformer.avro.RecordKey;
import com.example.transformer.service.AnimalEnrichmentService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Function;

@Configuration
public class AnimalTransformerStream {

    private final AnimalEnrichmentService enrichmentService;

    public AnimalTransformerStream(AnimalEnrichmentService enrichmentService) {
        this.enrichmentService = enrichmentService;
    }

    @Bean
    public Function<KStream<GenericRecord, GenericRecord>, KStream<RecordKey, AnimalDetails>> animalTransform() {
        return stream -> stream
                .filter((key, value) -> value != null)
                .mapValues(enrichmentService::extractAndEnrich)
                .filter((key, value) -> value != null)
                .selectKey((key, value) -> RecordKey.newBuilder().setId(value.getId()).build());
    }
}
