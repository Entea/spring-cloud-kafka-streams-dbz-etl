package com.example.transformer.stream;

import com.example.transformer.avro.EventDetails;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
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
    public Function<KStream<String, GenericRecord>, KStream<String, EventDetails>> eventTransform() {
        return stream -> stream
                .filter((key, value) -> value != null)
                .mapValues(this::extractAndEnrichEvent)
                .filter((key, value) -> value != null);
    }

    private EventDetails extractAndEnrichEvent(GenericRecord envelope) {
        try {
            logger.info("Got Debezium Avro envelope");

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

            long eventId = ((Number) idObj).longValue();
            if (eventId == 0) {
                logger.warn("Could not extract event ID from CDC payload");
                return null;
            }

            logger.info("Fetching event details for ID: {}", eventId);
            String eventJson = fetchEventFromApp(eventId);
            if (eventJson == null) {
                return null;
            }

            JsonNode node = objectMapper.readTree(eventJson);
            EventDetails enriched = EventDetails.newBuilder()
                    .setId(node.path("id").asLong())
                    .setVersion(node.path("version").asLong())
                    .setName(node.path("name").asText(""))
                    .build();

            logger.info("Successfully enriched event ID: {}", eventId);
            return enriched;
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
