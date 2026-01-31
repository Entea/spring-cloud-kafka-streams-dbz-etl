package com.example.transformer.service;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsRegistry;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
public class StreamControlService {

    private static final Logger logger = LoggerFactory.getLogger(StreamControlService.class);
    private static final int STREAM_STOP_WAIT_SECONDS = 5;
    private static final int CONSUMER_GROUP_INACTIVE_TIMEOUT_SECONDS = 60;
    private static final int POLL_INTERVAL_MS = 500;

    private final KafkaStreamsRegistry kafkaStreamsRegistry;
    private final KafkaOffsetService offsetService;
    private final BindingsLifecycleController bindingsController;

    public StreamControlService(KafkaStreamsRegistry kafkaStreamsRegistry,
                                 KafkaOffsetService offsetService,
                                 BindingsLifecycleController bindingsController) {
        this.kafkaStreamsRegistry = kafkaStreamsRegistry;
        this.offsetService = offsetService;
        this.bindingsController = bindingsController;
    }

    public Optional<StreamsBuilderFactoryBean> getStreamsFactoryBean(String applicationId) {
        try {
            StreamsBuilderFactoryBean factoryBean = kafkaStreamsRegistry.streamsBuilderFactoryBean(applicationId);
            return Optional.ofNullable(factoryBean);
        } catch (IllegalStateException e) {
            logger.debug("StreamsBuilderFactoryBean not found for applicationId: {}", applicationId);
            return Optional.empty();
        }
    }

    public boolean isStreamRunning(String applicationId) {
        return getStreamsFactoryBean(applicationId)
                .map(StreamsBuilderFactoryBean::isRunning)
                .orElse(false);
    }

    public KafkaStreams.State getStreamState(String applicationId) {
        return getStreamsFactoryBean(applicationId)
                .map(StreamsBuilderFactoryBean::getKafkaStreams)
                .map(KafkaStreams::state)
                .orElse(null);
    }

    public boolean stopStream(String applicationId, String inputBinding, String outputBinding) {
        if (!isStreamRunning(applicationId)) {
            logger.info("Stream {} is already stopped", applicationId);
            return true;
        }

        logger.info("Stopping Kafka Streams bindings for applicationId: {}", applicationId);
        try {
            bindingsController.changeState(inputBinding, State.STOPPED);
            if (outputBinding != null) {
                bindingsController.changeState(outputBinding, State.STOPPED);
            }
            logger.info("Kafka Streams stopped for applicationId: {}", applicationId);
            return true;
        } catch (Exception e) {
            logger.error("Failed to stop stream {}: {}", applicationId, e.getMessage(), e);
            return false;
        }
    }

    public boolean startStream(String applicationId, String inputBinding, String outputBinding) {
        if (isStreamRunning(applicationId)) {
            logger.info("Stream {} is already running", applicationId);
            return true;
        }

        logger.info("Starting Kafka Streams bindings for applicationId: {}", applicationId);
        try {
            bindingsController.changeState(inputBinding, State.STARTED);
            if (outputBinding != null) {
                bindingsController.changeState(outputBinding, State.STARTED);
            }
            logger.info("Kafka Streams started for applicationId: {}", applicationId);
            return true;
        } catch (Exception e) {
            logger.error("Failed to start stream {}: {}", applicationId, e.getMessage(), e);
            return false;
        }
    }

    public OffsetResetResult resetOffsets(String applicationId, String consumerGroup, String topic,
                                           String inputBinding, String outputBinding, OffsetResetType resetType)
            throws Exception {
        // Check if stream is running
        boolean wasRunning = isStreamRunning(applicationId);

        // Stop the stream if running
        if (wasRunning) {
            if (!stopStream(applicationId, inputBinding, outputBinding)) {
                return new OffsetResetResult(false, "Failed to stop stream", null, null);
            }

            // Wait for stream to fully stop
            waitForStreamStop(applicationId);

            // Wait for consumer group to become inactive (best effort)
            waitForConsumerGroupInactive(consumerGroup);
        }

        try {
            // Get current offsets before reset
            Map<TopicPartition, Long> previousOffsets = offsetService.getCommittedOffsets(consumerGroup);

            // Calculate new offsets based on reset type
            Map<TopicPartition, Long> newOffsets = switch (resetType) {
                case EARLIEST -> offsetService.getEarliestOffsets(topic);
                case LATEST -> offsetService.getLatestOffsets(topic);
            };

            // Reset the offsets
            offsetService.resetOffsets(consumerGroup, newOffsets);

            return new OffsetResetResult(true,
                    "Offsets reset successfully to " + resetType.name().toLowerCase(),
                    previousOffsets, newOffsets);
        } finally {
            // Restart stream if it was running before
            if (wasRunning) {
                startStream(applicationId, inputBinding, outputBinding);
            }
        }
    }

    public OffsetResetResult resetOffsetsToSpecific(String applicationId, String consumerGroup,
                                                     String inputBinding, String outputBinding,
                                                     Map<TopicPartition, Long> targetOffsets) throws Exception {
        // Check if stream is running
        boolean wasRunning = isStreamRunning(applicationId);

        // Stop the stream if running
        if (wasRunning) {
            if (!stopStream(applicationId, inputBinding, outputBinding)) {
                return new OffsetResetResult(false, "Failed to stop stream", null, null);
            }

            // Wait for stream to fully stop
            waitForStreamStop(applicationId);

            // Wait for consumer group to become inactive (best effort)
            waitForConsumerGroupInactive(consumerGroup);
        }

        try {
            // Get current offsets before reset
            Map<TopicPartition, Long> previousOffsets = offsetService.getCommittedOffsets(consumerGroup);

            // Reset the offsets
            offsetService.resetOffsets(consumerGroup, targetOffsets);

            return new OffsetResetResult(true,
                    "Offsets reset successfully to specified values",
                    previousOffsets, targetOffsets);
        } finally {
            // Restart stream if it was running before
            if (wasRunning) {
                startStream(applicationId, inputBinding, outputBinding);
            }
        }
    }

    private void waitForStreamStop(String applicationId) {
        long startTime = System.currentTimeMillis();
        long timeoutMs = TimeUnit.SECONDS.toMillis(STREAM_STOP_WAIT_SECONDS);

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            if (!isStreamRunning(applicationId)) {
                logger.info("Stream {} has stopped", applicationId);
                return;
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        logger.info("Stream stop wait completed for {}", applicationId);
    }

    private boolean waitForConsumerGroupInactive(String consumerGroup) {
        long startTime = System.currentTimeMillis();
        long timeoutMs = TimeUnit.SECONDS.toMillis(CONSUMER_GROUP_INACTIVE_TIMEOUT_SECONDS);

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            if (!offsetService.isConsumerGroupActive(consumerGroup)) {
                logger.info("Consumer group {} is now inactive", consumerGroup);
                return true;
            }

            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        logger.warn("Timed out waiting for consumer group {} to become inactive", consumerGroup);
        return false;
    }

    public enum OffsetResetType {
        EARLIEST,
        LATEST
    }

    public record OffsetResetResult(
            boolean success,
            String message,
            Map<TopicPartition, Long> previousOffsets,
            Map<TopicPartition, Long> newOffsets
    ) {}
}
