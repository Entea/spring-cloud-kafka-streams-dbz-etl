package com.example.transformer.dto;

import java.time.Instant;

public record StreamStatusResponse(
        String applicationId,
        boolean running,
        String state,
        Instant timestamp
) {}
