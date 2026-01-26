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
public class EventTransformerStream {

    private static final Logger logger = LoggerFactory.getLogger(EventTransformerStream.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${app.service.url}")
    private String appServiceUrl;

    @Value("${app.topics.input}")
    private String inputTopic;

    @Value("${app.topics.output}")
    private String outputTopic;

    @Bean
    public KStream<String, String> eventStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> transformedStream = stream
                .filter((key, value) -> value != null)
                .mapValues(this::extractAndEnrichEvent)
                .filter((key, value) -> value != null);

        transformedStream.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return transformedStream;
    }

    private String extractAndEnrichEvent(String cdcPayload) {
        try {
            JsonNode root = objectMapper.readTree(cdcPayload);
            JsonNode payload = root.path("payload");

            JsonNode after = payload.path("after");
            if (after.isMissingNode() || after.isNull()) {
                logger.debug("No 'after' field in CDC payload, skipping delete event");
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
            return null;
        }
    }

    private String fetchEventFromApp(Long eventId) {
        try {
            String url = appServiceUrl + "/api/events/" + eventId;
            return restTemplate.getForObject(url, String.class);
        } catch (Exception e) {
            logger.error("Failed to fetch event {} from app service: {}", eventId, e.getMessage());
            return null;
        }
    }
}
