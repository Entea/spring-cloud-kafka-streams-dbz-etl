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
public class EventTransformerStream {

    private static final Logger logger = LoggerFactory.getLogger(EventTransformerStream.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${app.service.url}")
    private String appServiceUrl;

    @Bean
    public Function<KStream<String, String>, KStream<String, String>> eventTransform() {
        return stream -> stream
                .filter((key, value) -> value != null)
                .mapValues(this::extractAndEnrichEvent)
                .filter((key, value) -> value != null);
    }

    private String extractAndEnrichEvent(String cdcPayload) {
        try {
            logger.info("Got payload {}", cdcPayload);
            JsonNode root = objectMapper.readTree(cdcPayload);

            JsonNode after = root.path("after");
            if (after.isMissingNode() || after.isNull()) {
                logger.warn("No 'after' field in CDC payload, skipping delete event");
                return null;
            }

            Long eventId = after.path("id").asLong();
            if (eventId == 0) {
                logger.warn("Could not extract event ID from CDC payload");
                return null;
            }

            logger.info("Fetching event details for ID: {}", eventId);
            String eventDetails = fetchEventFromApp(eventId);

            if (eventDetails != null) {
                logger.info("Successfully enriched event ID: {}", eventId);
            }

            return eventDetails;
        } catch (Exception e) {
            logger.error("Error processing CDC payload: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process CDC payload", e);
        }
    }

    private String fetchEventFromApp(Long eventId) {
        String url = appServiceUrl + "/api/events/" + eventId;
        return restTemplate.getForObject(url, String.class);
    }
}
