package com.example.transformer.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaOffsetService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaOffsetService.class);

    private final AdminClient adminClient;

    public KafkaOffsetService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public Map<TopicPartition, Long> getCommittedOffsets(String consumerGroup)
            throws ExecutionException, InterruptedException {
        var offsets = adminClient.listConsumerGroupOffsets(consumerGroup)
                .partitionsToOffsetAndMetadata()
                .get();

        return offsets.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().offset()
                ));
    }

    public Map<TopicPartition, Long> getEndOffsets(Collection<TopicPartition> partitions)
            throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> offsetSpecs = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

        ListOffsetsResult result = adminClient.listOffsets(offsetSpecs);

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        for (TopicPartition tp : partitions) {
            endOffsets.put(tp, result.partitionResult(tp).get().offset());
        }
        return endOffsets;
    }

    public Map<TopicPartition, Long> getEarliestOffsets(String topic)
            throws ExecutionException, InterruptedException {
        var partitions = getTopicPartitions(topic);

        Map<TopicPartition, OffsetSpec> offsetSpecs = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()));

        ListOffsetsResult result = adminClient.listOffsets(offsetSpecs);

        Map<TopicPartition, Long> earliestOffsets = new HashMap<>();
        for (TopicPartition tp : partitions) {
            earliestOffsets.put(tp, result.partitionResult(tp).get().offset());
        }
        return earliestOffsets;
    }

    public Map<TopicPartition, Long> getLatestOffsets(String topic)
            throws ExecutionException, InterruptedException {
        var partitions = getTopicPartitions(topic);
        return getEndOffsets(partitions);
    }

    public void resetOffsets(String consumerGroup, Map<TopicPartition, Long> offsets)
            throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = offsets.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new OffsetAndMetadata(e.getValue())
                ));

        adminClient.alterConsumerGroupOffsets(consumerGroup, offsetsToCommit).all().get();
        logger.info("Reset offsets for consumer group {} to: {}", consumerGroup, offsets);
    }

    public boolean isConsumerGroupActive(String consumerGroup) {
        try {
            ConsumerGroupDescription description = adminClient
                    .describeConsumerGroups(java.util.List.of(consumerGroup))
                    .describedGroups()
                    .get(consumerGroup)
                    .get();

            ConsumerGroupState state = description.state();
            int memberCount = description.members().size();
            logger.debug("Consumer group {} state: {}, members: {}", consumerGroup, state, memberCount);

            // Group is active if it has members or is in a transitional state
            return memberCount > 0 ||
                   state == ConsumerGroupState.STABLE ||
                   state == ConsumerGroupState.PREPARING_REBALANCE ||
                   state == ConsumerGroupState.COMPLETING_REBALANCE;
        } catch (Exception e) {
            logger.debug("Could not describe consumer group {}: {}", consumerGroup, e.getMessage());
            return false; // Assume inactive if we can't determine state
        }
    }

    public Collection<TopicPartition> getTopicPartitions(String topic)
            throws ExecutionException, InterruptedException {
        return adminClient.describeTopics(java.util.List.of(topic))
                .allTopicNames()
                .get()
                .get(topic)
                .partitions()
                .stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .collect(Collectors.toList());
    }

}
