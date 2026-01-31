package com.example.transformer.controller;

import com.example.transformer.dto.OffsetInfoResponse;
import com.example.transformer.dto.OffsetResetRequest;
import com.example.transformer.dto.OffsetResetResponse;
import com.example.transformer.dto.PartitionOffsetInfo;
import com.example.transformer.dto.StreamStatusResponse;
import com.example.transformer.service.KafkaOffsetService;
import com.example.transformer.service.StreamControlService;
import com.example.transformer.service.StreamControlService.OffsetResetResult;
import com.example.transformer.service.StreamControlService.OffsetResetType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/stream/animal")
public class AnimalStreamController {

    private static final Logger logger = LoggerFactory.getLogger(AnimalStreamController.class);

    private static final String APPLICATION_ID = "animal-transformer";
    private static final String CONSUMER_GROUP = "animal-transformer";
    private static final String INPUT_TOPIC = "dbserver1.public.animal";
    private static final String INPUT_BINDING = "animalTransform-in-0";
    private static final String OUTPUT_BINDING = "animalTransform-out-0";

    private final KafkaOffsetService offsetService;
    private final StreamControlService streamControlService;

    public AnimalStreamController(KafkaOffsetService offsetService, StreamControlService streamControlService) {
        this.offsetService = offsetService;
        this.streamControlService = streamControlService;
    }

    @GetMapping("/offsets")
    public ResponseEntity<OffsetInfoResponse> getOffsets() {
        try {
            Map<TopicPartition, Long> committedOffsets = offsetService.getCommittedOffsets(CONSUMER_GROUP);

            Map<TopicPartition, Long> topicOffsets = committedOffsets.entrySet().stream()
                    .filter(e -> e.getKey().topic().equals(INPUT_TOPIC))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // If no committed offsets for this topic, get partitions directly
            var partitions = topicOffsets.isEmpty()
                    ? offsetService.getTopicPartitions(INPUT_TOPIC)
                    : topicOffsets.keySet();

            Map<TopicPartition, Long> endOffsets = offsetService.getEndOffsets(partitions);

            List<PartitionOffsetInfo> partitionInfos = new ArrayList<>();
            for (TopicPartition tp : partitions) {
                long committed = topicOffsets.getOrDefault(tp, 0L);
                long end = endOffsets.getOrDefault(tp, 0L);
                partitionInfos.add(new PartitionOffsetInfo(
                        tp.partition(),
                        committed,
                        end,
                        end - committed
                ));
            }

            partitionInfos.sort((a, b) -> Integer.compare(a.partition(), b.partition()));

            return ResponseEntity.ok(new OffsetInfoResponse(
                    CONSUMER_GROUP,
                    INPUT_TOPIC,
                    partitionInfos,
                    Instant.now()
            ));
        } catch (Exception e) {
            logger.error("Failed to get offsets", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/status")
    public ResponseEntity<StreamStatusResponse> getStatus() {
        boolean running = streamControlService.isStreamRunning(APPLICATION_ID);
        KafkaStreams.State state = streamControlService.getStreamState(APPLICATION_ID);
        return ResponseEntity.ok(new StreamStatusResponse(
                APPLICATION_ID,
                running,
                state != null ? state.name() : "UNKNOWN",
                Instant.now()
        ));
    }

    @PostMapping("/offsets/reset")
    public ResponseEntity<OffsetResetResponse> resetOffsets(@RequestBody OffsetResetRequest request) {
        try {
            Map<TopicPartition, Long> targetOffsets = new HashMap<>();
            for (Map.Entry<String, Long> entry : request.partitionOffsets().entrySet()) {
                int partition = Integer.parseInt(entry.getKey());
                targetOffsets.put(new TopicPartition(INPUT_TOPIC, partition), entry.getValue());
            }

            OffsetResetResult result = streamControlService.resetOffsetsToSpecific(
                    APPLICATION_ID, CONSUMER_GROUP, INPUT_BINDING, OUTPUT_BINDING, targetOffsets);

            return buildOffsetResetResponse(result);
        } catch (Exception e) {
            logger.error("Failed to reset offsets", e);
            return ResponseEntity.internalServerError()
                    .body(new OffsetResetResponse(false, "Error: " + e.getMessage(), null, null));
        }
    }

    @PostMapping("/offsets/reset/earliest")
    public ResponseEntity<OffsetResetResponse> resetToEarliest() {
        try {
            OffsetResetResult result = streamControlService.resetOffsets(
                    APPLICATION_ID, CONSUMER_GROUP, INPUT_TOPIC, INPUT_BINDING, OUTPUT_BINDING, OffsetResetType.EARLIEST);

            return buildOffsetResetResponse(result);
        } catch (Exception e) {
            logger.error("Failed to reset offsets to earliest", e);
            return ResponseEntity.internalServerError()
                    .body(new OffsetResetResponse(false, "Error: " + e.getMessage(), null, null));
        }
    }

    @PostMapping("/offsets/reset/latest")
    public ResponseEntity<OffsetResetResponse> resetToLatest() {
        try {
            OffsetResetResult result = streamControlService.resetOffsets(
                    APPLICATION_ID, CONSUMER_GROUP, INPUT_TOPIC, INPUT_BINDING, OUTPUT_BINDING, OffsetResetType.LATEST);

            return buildOffsetResetResponse(result);
        } catch (Exception e) {
            logger.error("Failed to reset offsets to latest", e);
            return ResponseEntity.internalServerError()
                    .body(new OffsetResetResponse(false, "Error: " + e.getMessage(), null, null));
        }
    }

    @PostMapping("/stop")
    public ResponseEntity<StreamStatusResponse> stopStream() {
        logger.info("Stream stop requested for {}", APPLICATION_ID);
        boolean stopped = streamControlService.stopStream(APPLICATION_ID, INPUT_BINDING, OUTPUT_BINDING);

        if (stopped) {
            KafkaStreams.State state = streamControlService.getStreamState(APPLICATION_ID);
            return ResponseEntity.ok(new StreamStatusResponse(
                    APPLICATION_ID,
                    false,
                    state != null ? state.name() : "NOT_RUNNING",
                    Instant.now()
            ));
        } else {
            return ResponseEntity.internalServerError()
                    .body(new StreamStatusResponse(APPLICATION_ID, false, "STOP_FAILED", Instant.now()));
        }
    }

    @PostMapping("/start")
    public ResponseEntity<StreamStatusResponse> startStream() {
        logger.info("Stream start requested for {}", APPLICATION_ID);
        boolean started = streamControlService.startStream(APPLICATION_ID, INPUT_BINDING, OUTPUT_BINDING);

        if (started) {
            KafkaStreams.State state = streamControlService.getStreamState(APPLICATION_ID);
            return ResponseEntity.ok(new StreamStatusResponse(
                    APPLICATION_ID,
                    true,
                    state != null ? state.name() : "RUNNING",
                    Instant.now()
            ));
        } else {
            return ResponseEntity.internalServerError()
                    .body(new StreamStatusResponse(APPLICATION_ID, false, "START_FAILED", Instant.now()));
        }
    }

    private ResponseEntity<OffsetResetResponse> buildOffsetResetResponse(OffsetResetResult result) {
        Map<String, Long> prevOffsets = result.previousOffsets() != null
                ? result.previousOffsets().entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> String.valueOf(e.getKey().partition()),
                                Map.Entry::getValue))
                : null;

        Map<String, Long> newOffsets = result.newOffsets() != null
                ? result.newOffsets().entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> String.valueOf(e.getKey().partition()),
                                Map.Entry::getValue))
                : null;

        OffsetResetResponse response = new OffsetResetResponse(
                result.success(),
                result.message(),
                prevOffsets,
                newOffsets
        );

        if (result.success()) {
            return ResponseEntity.ok(response);
        } else {
            return ResponseEntity.status(409).body(response); // Conflict - could not reset
        }
    }
}
