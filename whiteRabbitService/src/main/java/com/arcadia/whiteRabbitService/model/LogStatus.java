package com.arcadia.whiteRabbitService.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum LogStatus {
    INFO(1, "INFO"),
    DEBUG(2, "DEBUG"),
    WARNING(3, "WARNING"),
    ERROR(4, "ERROR");

    private final int code;
    private final String name;
}
