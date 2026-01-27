package com.example.transformer.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class AnimalTransformerStream {

    private static final Logger logger = LoggerFactory.getLogger(AnimalTransformerStream.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${app.service.url}")
    private String appServiceUrl;

    @Value("${app.topics.animal-input}")
    private String inputTopic;

    @Value("${app.topics.animal-output}")
    private String outputTopic;

    @Bean
    public KStream<String, String> animalStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> transformedStream = stream
                .filter((key, value) -> value != null)
                .mapValues(this::extractAndEnrichAnimal)
                .filter((key, value) -> value != null);

        transformedStream.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return transformedStream;
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
            return null;
        }
    }

    private String fetchAnimalFromApp(Long animalId) {
        try {
            String url = appServiceUrl + "/api/animals/" + animalId;
            return restTemplate.getForObject(url, String.class);
        } catch (Exception e) {
            logger.error("Failed to fetch animal {} from app service: {}", animalId, e.getMessage());
            return null;
        }
    }
}
