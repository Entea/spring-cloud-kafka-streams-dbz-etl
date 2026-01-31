package com.example.transformer.dto;

public record PartitionOffsetInfo(
        int partition,
        long committedOffset,
        long endOffset,
        long lag
) {}
