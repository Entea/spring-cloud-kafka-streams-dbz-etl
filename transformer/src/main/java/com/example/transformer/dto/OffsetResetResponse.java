package com.example.transformer.dto;

import java.util.Map;

public record OffsetResetResponse(
        boolean success,
        String message,
        Map<String, Long> previousOffsets,
        Map<String, Long> newOffsets
) {}
