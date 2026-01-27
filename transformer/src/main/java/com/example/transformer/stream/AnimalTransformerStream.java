package com.example.transformer.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.util.function.Function;

@Configuration
public class AnimalTransformerStream {

    private static final Logger logger = LoggerFactory.getLogger(AnimalTransformerStream.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${app.service.url}")
    private String appServiceUrl;

    @Bean
    public Function<KStream<String, String>, KStream<String, String>> animalTransform() {
        return stream -> stream
                .filter((key, value) -> value != null)
                .mapValues(this::extractAndEnrichAnimal)
                .filter((key, value) -> value != null);
    }

    private String extractAndEnrichAnimal(String cdcPayload) {
        try {
            logger.info("Got payload {}", cdcPayload);
            JsonNode root = objectMapper.readTree(cdcPayload);

            JsonNode after = root.path("after");
            if (after.isMissingNode() || after.isNull()) {
                logger.warn("No 'after' field in CDC payload, skipping delete event");
                return null;
            }

            Long animalId = after.path("id").asLong();
            if (animalId == 0) {
                logger.warn("Could not extract animal ID from CDC payload");
                return null;
            }

            logger.info("Fetching animal details for ID: {}", animalId);
            String animalDetails = fetchAnimalFromApp(animalId);

            if (animalDetails != null) {
                logger.info("Successfully enriched animal ID: {}", animalId);
            }

            return animalDetails;
        } catch (Exception e) {
            logger.error("Error processing CDC payload: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process CDC payload", e);
        }
    }

    private String fetchAnimalFromApp(Long animalId) {
        String url = appServiceUrl + "/api/animals/" + animalId;
        return restTemplate.getForObject(url, String.class);
    }
}
