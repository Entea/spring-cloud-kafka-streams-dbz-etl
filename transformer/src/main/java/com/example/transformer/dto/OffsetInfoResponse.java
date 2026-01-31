package com.example.transformer.dto;

import java.time.Instant;
import java.util.List;

public record OffsetInfoResponse(
        String consumerGroup,
        String topic,
        List<PartitionOffsetInfo> partitionOffsets,
        Instant timestamp
) {}
