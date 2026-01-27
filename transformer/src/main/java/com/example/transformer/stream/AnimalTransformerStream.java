package com.example.transformer.stream;

import com.example.transformer.service.AnimalEnrichmentService;
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
    public Function<KStream<String, String>, KStream<String, String>> animalTransform() {
        return stream -> stream
                .filter((key, value) -> value != null)
                .mapValues(enrichmentService::extractAndEnrich)
                .filter((key, value) -> value != null);
    }
}
