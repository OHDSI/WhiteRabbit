package com.arcadia.whiteRabbitService.service.util;

import com.arcadia.whiteRabbitService.model.LogStatus;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataLog;

import java.sql.Timestamp;

import static com.arcadia.whiteRabbitService.util.LoggerUtil.checkMessageAndSubstringIfNeeded;

public record FakeDataLogCreator(FakeDataConversion conversion) implements LogCreator<FakeDataLog> {
    @Override
    public FakeDataLog create(String message, LogStatus status, Integer percent) {
        return FakeDataLog.builder()
                .message(checkMessageAndSubstringIfNeeded(message))
                .time(new Timestamp(System.currentTimeMillis()))
                .statusCode(status.getCode())
                .statusName(status.getName())
                .percent(percent)
                .fakeDataConversion(conversion)
                .build();
    }
}
