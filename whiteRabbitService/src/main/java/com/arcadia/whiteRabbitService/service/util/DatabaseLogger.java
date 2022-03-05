package com.arcadia.whiteRabbitService.service.util;

import com.arcadia.whiteRabbitService.model.LogStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.ohdsi.whiteRabbit.Logger;
import org.springframework.data.jpa.repository.JpaRepository;

import static com.arcadia.whiteRabbitService.model.LogStatus.*;

@Slf4j
@RequiredArgsConstructor
public class DatabaseLogger<T> implements Logger {
    private final JpaRepository<T, Long> logRepository;
    private final LogCreator<T> logCreator;

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
        T log = logCreator.create(message, status, percent);
        logRepository.save(log);
    }

    private int currentPercent() {
        double percent = ((double) scannedItemsCount / itemsCount) * 100;
        return (int) percent;
    }

    public void setItemsCount(int itemsCount) {
        this.itemsCount = itemsCount;
    }
}
