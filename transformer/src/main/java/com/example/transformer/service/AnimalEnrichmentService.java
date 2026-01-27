package com.example.transformer.service;

import com.example.transformer.avro.AnimalDetails;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class AnimalEnrichmentService {

    private static final Logger logger = LoggerFactory.getLogger(AnimalEnrichmentService.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${app.service.url}")
    private String appServiceUrl;

    public AnimalDetails extractAndEnrich(GenericRecord envelope) {
        logger.info("Got Debezium Avro envelope for animal");

        Object afterObj = envelope.get("after");
        if (afterObj == null) {
            logger.warn("No 'after' field in CDC payload, skipping delete event");
            return null;
        }

        GenericRecord after = (GenericRecord) afterObj;
        Object idObj = after.get("id");
        if (idObj == null) {
            logger.warn("Could not find 'id' field in after record");
            return null;
        }

        long animalId = ((Number) idObj).longValue();
        if (animalId == 0) {
            logger.warn("Could not extract animal ID from CDC payload");
            return null;
        }

        logger.info("Fetching animal details for ID: {}", animalId);
        String animalJson = fetchAnimalFromApp(animalId);
        if (animalJson == null) {
            return null;
        }

        try {
            JsonNode node = objectMapper.readTree(animalJson);
            AnimalDetails enriched = AnimalDetails.newBuilder()
                    .setId(node.path("id").asLong())
                    .setVersion(node.path("version").asLong())
                    .setName(node.path("name").asText(""))
                    .setBreed(node.path("breed").asText(""))
                    .build();

            logger.info("Successfully enriched animal ID: {}", animalId);
            return enriched;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build enriched animal", e);
        }
    }

    public String fetchAnimalFromApp(Long animalId) {
        String url = appServiceUrl + "/api/animals/" + animalId;
        return restTemplate.getForObject(url, String.class);
    }
}
