package com.example.transformer.dto;

import java.util.Map;

public record OffsetResetRequest(
        Map<String, Long> partitionOffsets
) {}
