package com.arcadia.whiteRabbitService.util;

import com.arcadia.whiteRabbitService.model.Log;
import com.arcadia.whiteRabbitService.service.response.LogResponse;

public class LogUtil {
    public static LogResponse toResponse(Log log) {
        return LogResponse.builder()
                .message(log.getMessage())
                .statusCode(log.getStatusCode())
                .statusName(log.getStatusName())
                .percent(log.getPercent())
                .build();
    }
}
