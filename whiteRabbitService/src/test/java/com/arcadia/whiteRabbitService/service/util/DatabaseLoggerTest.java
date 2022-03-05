package com.arcadia.whiteRabbitService.service.util;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataLog;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static com.arcadia.whiteRabbitService.model.LogStatus.*;
import static com.arcadia.whiteRabbitService.service.ScanDataConversionServiceTest.createScanDataConversion;
import static com.arcadia.whiteRabbitService.util.LoggerUtil.LOG_MESSAGE_MAX_LENGTH;
import static com.arcadia.whiteRabbitService.util.LoggerUtilTest.generateRandomString;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class DatabaseLoggerTest {
    @Autowired
    ScanDataLogRepository logRepository;

    @Autowired
    ScanDataConversionRepository conversionRepository;

    DatabaseLogger<ScanDataLog> databaseLogger;

    @BeforeEach
    void setUp() {
        ScanDataConversion conversion = conversionRepository.save(createScanDataConversion());
        ScanDataLogCreator logCreator = new ScanDataLogCreator(conversion);
        databaseLogger = new DatabaseLogger<>(logRepository, logCreator);
    }

    @Test
    void logInfoMessage() {
        String message = "Test";
        databaseLogger.info(message);
        List<ScanDataLog> logs = logRepository.findAll();
        ScanDataLog log = logs.get(logs.size() - 1);

        assertEquals(message, log.getMessage());
        assertEquals(INFO.getCode(), log.getStatusCode());
        assertEquals(INFO.getName(), log.getStatusName());
    }

    @Test
    void logDebugMessage() {
        String message = "Test";
        databaseLogger.debug(message);
        List<ScanDataLog> logs = logRepository.findAll();
        ScanDataLog log = logs.get(logs.size() - 1);

        assertEquals(message, log.getMessage());
        assertEquals(DEBUG.getCode(), log.getStatusCode());
        assertEquals(DEBUG.getName(), log.getStatusName());
    }

    @Test
    void logWarningMessage() {
        String message = "Test";
        databaseLogger.warning(message);
        List<ScanDataLog> logs = logRepository.findAll();
        ScanDataLog log = logs.get(logs.size() - 1);

        assertEquals(message, log.getMessage());
        assertEquals(WARNING.getCode(), log.getStatusCode());
        assertEquals(WARNING.getName(), log.getStatusName());
    }

    @Test
    void logErrorMessage() {
        String message = "Test";
        databaseLogger.error(message);
        List<ScanDataLog> logs = logRepository.findAll();
        ScanDataLog log = logs.get(logs.size() - 1);

        assertEquals(message, log.getMessage());
        assertEquals(ERROR.getCode(), log.getStatusCode());
        assertEquals(ERROR.getName(), log.getStatusName());
    }

    @Test
    void logLongMessage() {
        int messageLength = LOG_MESSAGE_MAX_LENGTH + 100;
        String message = generateRandomString(messageLength);

        databaseLogger.info(message);
        List<ScanDataLog> logs = logRepository.findAll();
        ScanDataLog log = logs.get(logs.size() - 1);

        String expectedMessage = message.substring(0, LOG_MESSAGE_MAX_LENGTH);

        assertEquals(expectedMessage, log.getMessage());
    }
}