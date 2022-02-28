package com.arcadia.whiteRabbitService.util;

import com.arcadia.whiteRabbitService.model.LogStatus;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataLog;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import lombok.RequiredArgsConstructor;
import org.ohdsi.utilities.Logger;

import java.sql.Timestamp;

import static com.arcadia.whiteRabbitService.model.LogStatus.*;

@RequiredArgsConstructor
public class DatabaseLogger implements Logger {
    private final ScanDataLogRepository logRepository;
    private final ScanDataConversion scanDataConversion;

    @Override
    public void info(String message) {
        saveLogMessageWithStatus(message, INFO);
    }

    @Override
    public void debug(String message) {
        saveLogMessageWithStatus(message, DEBUG);
    }

    @Override
    public void warning(String message) {
        saveLogMessageWithStatus(message, WARNING);
    }

    @Override
    public void error(String message) {
        saveLogMessageWithStatus(message, ERROR);
    }

    private void saveLogMessageWithStatus(String message, LogStatus status) {
        ScanDataLog log = ScanDataLog.builder()
                .message(message)
                .time(new Timestamp(System.currentTimeMillis()))
                .statusCode(status.getCode())
                .statusName(status.getName())
                .scanDataConversion(scanDataConversion)
                .build();
        logRepository.save(log);
    }
}
