package com.arcadia.whiteRabbitService.util;

import com.arcadia.whiteRabbitService.model.Conversion;
import com.arcadia.whiteRabbitService.model.Log;
import com.arcadia.whiteRabbitService.service.response.ConversionWithLogsResponse;

import java.util.stream.Collectors;

public class ConversionUtil {
    public static <T extends Log> ConversionWithLogsResponse toResponseWithLogs(Conversion<T> conversion) {
        return ConversionWithLogsResponse.builder()
                .id(conversion.getId())
                .statusCode(conversion.getStatusCode())
                .statusName(conversion.getStatusName())
                .logs(conversion.getLogs().stream().map(LogUtil::toResponse).collect(Collectors.toList()))
                .build();
    }
}
