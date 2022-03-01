package com.arcadia.whiteRabbitService.service.util;

import com.arcadia.whiteRabbitService.model.LogStatus;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataLog;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.ohdsi.whiteRabbit.Logger;

import java.sql.Timestamp;

import static com.arcadia.whiteRabbitService.model.LogStatus.*;
import static com.arcadia.whiteRabbitService.util.LoggerUtil.checkMessageAndSubstringIfNeeded;

@Slf4j
@RequiredArgsConstructor
public class DatabaseLogger implements Logger {
    private final ScanDataLogRepository logRepository;
    private final ScanDataConversion scanDataConversion;

    private int itemsCount = 0;
    private int scannedItemsCount = 0;

    @Override
    public void info(String message) {
        saveLogMessageWithStatus(message, INFO, currentPercent());
    }

    @Override
    public void debug(String message) {
        saveLogMessageWithStatus(message, DEBUG, currentPercent());
    }

    @Override
    public void warning(String message) {
        saveLogMessageWithStatus(message, WARNING, currentPercent());
    }

    @Override
    public void error(String message) {
        saveLogMessageWithStatus(message, ERROR, 100);
    }

    @Override
    public void incrementScannedItems() {
        this.scannedItemsCount++;
    }

    private void saveLogMessageWithStatus(String message, LogStatus status, Integer percent) {
        log.info(message);
        ScanDataLog scanDataLog = ScanDataLog.builder()
                .message(checkMessageAndSubstringIfNeeded(message))
                .time(new Timestamp(System.currentTimeMillis()))
                .statusCode(status.getCode())
                .statusName(status.getName())
                .percent(percent)
                .scanDataConversion(scanDataConversion)
                .build();
        logRepository.save(scanDataLog);
    }

    private int currentPercent() {
        double percent = ((double) scannedItemsCount / itemsCount) * 100;
        return (int) percent;
    }

    public void setItemsCount(int itemsCount) {
        this.itemsCount = itemsCount;
    }
}
