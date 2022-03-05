package com.arcadia.whiteRabbitService.service.util;

import com.arcadia.whiteRabbitService.model.LogStatus;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataLog;

import java.sql.Timestamp;

import static com.arcadia.whiteRabbitService.util.LoggerUtil.checkMessageAndSubstringIfNeeded;

public record ScanDataLogCreator(ScanDataConversion scanDataConversion) implements LogCreator<ScanDataLog> {
    @Override
    public ScanDataLog create(String message, LogStatus status, Integer percent) {
        return ScanDataLog.builder()
                .message(checkMessageAndSubstringIfNeeded(message))
                .time(new Timestamp(System.currentTimeMillis()))
                .statusCode(status.getCode())
                .statusName(status.getName())
                .percent(percent)
                .scanDataConversion(scanDataConversion)
                .build();
    }
}
