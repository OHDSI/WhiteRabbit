package com.arcadia.whiteRabbitService.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ConversionStatus {
    IN_PROGRESS(1, "IN_PROGRESS"),
    COMPLETED(2, "COMPLETED"),
    ABORTED(3, "ABORTED"),
    FAILED(4, "FAILED");

    private final int code;
    private final String name;
}
