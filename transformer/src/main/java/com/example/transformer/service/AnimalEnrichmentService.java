package com.example.transformer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    public String extractAndEnrich(String cdcPayload) {
        logger.info("Got payload {}", cdcPayload);
        JsonNode root;
        try {
            root = objectMapper.readTree(cdcPayload);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse CDC payload", e);
        }

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
    }

    public String fetchAnimalFromApp(Long animalId) {
        String url = appServiceUrl + "/api/animals/" + animalId;
        return restTemplate.getForObject(url, String.class);
    }
}
