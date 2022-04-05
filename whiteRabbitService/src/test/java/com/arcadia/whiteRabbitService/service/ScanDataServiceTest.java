package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.ConversionStatus;
import com.arcadia.whiteRabbitService.model.LogStatus;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataLog;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataResult;
import com.arcadia.whiteRabbitService.model.scandata.ScanDbSettings;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataResultRepository;
import com.arcadia.whiteRabbitService.service.response.ConversionWithLogsResponse;
import com.arcadia.whiteRabbitService.service.response.FileSaveResponse;
import com.arcadia.whiteRabbitService.util.LogUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.*;
import static com.arcadia.whiteRabbitService.model.LogStatus.INFO;
import static com.arcadia.whiteRabbitService.service.DbSettingsAdapterTest.createTestDbSettings;
import static com.arcadia.whiteRabbitService.service.ScanDataConversionServiceTest.createScanDataConversion;
import static com.arcadia.whiteRabbitService.service.ScanDataResultServiceImpl.DATA_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@SpringBootTest
@ExtendWith(SpringExtension.class)
public class ScanDataServiceTest {
    @Autowired
    ScanDataConversionRepository conversionRepository;

    @Autowired
    StorageService storageService;

    @Autowired
    ScanDataLogRepository logRepository;

    @Autowired
    ScanDataResultRepository resultRepository;

    @MockBean
    WhiteRabbitFacade whiteRabbitFacade;

    @MockBean
    FilesManagerService filesManagerService;

    @MockBean
    File scanReportFile;

    ScanDataResultService resultService;
    ScanDataConversionService conversionService;
    ScanDataService scanDataService;

    @BeforeEach
    void setUp() {
        resultService = new ScanDataResultServiceImpl(
                conversionRepository,
                resultRepository,
                logRepository,
                filesManagerService
        );
        conversionService = new ScanDataConversionServiceImpl(
                logRepository,
                conversionRepository,
                whiteRabbitFacade,
                resultService
        );
        scanDataService = new ScanDataServiceImpl(
                conversionRepository,
                conversionService,
                storageService,
                logRepository,
                resultRepository
        );
    }

    @Transactional
    @Test
    void scanDatabaseDataAndComplete() throws InterruptedException {
        ScanDbSettings dbSettings = createTestDbSettings("postgresql", 5433);
        String username = "Perseus";
        Mockito.when(whiteRabbitFacade.generateScanReport(eq(dbSettings), any(), any()))
                .thenReturn(scanReportFile);
        Long id = 1L;
        FileSaveResponse fileSaveResponse = new FileSaveResponse(id, username, DATA_KEY, "test.xlsx");
        Mockito.when(filesManagerService.saveFile(any()))
                .thenReturn(fileSaveResponse);

        ScanDataConversion conversion = scanDataService.scanDatabaseData(dbSettings, username);

        assertEquals(COMPLETED.getCode(), conversion.getStatusCode());
        assertEquals(COMPLETED.getName(), conversion.getStatusName());
        assertEquals(username, conversion.getUsername());

        ScanDataResult conversionResult = conversion.getResult();
        assertEquals(id, conversionResult.getFileId());
        assertEquals(dbSettings.getDatabase() + ".xlsx", conversionResult.getFileName());
    }

    @Transactional
    @Test
    void scanDatabaseDataWithFailedResult() throws InterruptedException {
        ScanDbSettings dbSettings = createTestDbSettings("postgresql", 5433);
        String username = "Perseus";
        Mockito.when(whiteRabbitFacade.generateScanReport(eq(dbSettings), any(), any()))
                .thenThrow(new RuntimeException("Test error"));
        Long id = 1L;
        FileSaveResponse fileSaveResponse = new FileSaveResponse(id, username, DATA_KEY, "text.xls");
        Mockito.when(filesManagerService.saveFile(any()))
                .thenReturn(fileSaveResponse);

        ScanDataConversion conversion = scanDataService.scanDatabaseData(dbSettings, username);

        assertEquals(FAILED.getCode(), conversion.getStatusCode());
        assertEquals(FAILED.getName(), conversion.getStatusName());
        assertEquals(username, conversion.getUsername());
    }

    @Test
    void conversionInfoWithLogs() {
        ScanDataConversion conversion = createScanDataConversion();
        conversionRepository.save(conversion);
        ScanDataLog log1 = createScanDataLog(conversion, "Test", INFO, 1);
        ScanDataLog log2 = createScanDataLog(conversion, "Test", INFO, 2);
        ScanDataLog log3 = createScanDataLog(conversion, "Test", INFO, 3);
        List<ScanDataLog> logs = List.of(log1, log2, log3);
        logRepository.saveAll(logs);

        ConversionWithLogsResponse response =
                scanDataService.conversionInfoWithLogs(conversion.getId(), conversion.getUsername());

        assertEquals(conversion.getId(), response.getId());
        assertArrayEquals(logs.stream().map(LogUtil::toResponse).toArray(), response.getLogs().toArray());
    }

    public static ScanDataLog createScanDataLog(ScanDataConversion conversion,
                                                String message,
                                                LogStatus status,
                                                Integer percent) {
        return ScanDataLog.builder()
                .scanDataConversion(conversion)
                .message(message)
                .statusCode(status.getCode())
                .statusName(status.getName())
                .percent(percent)
                .time(new Timestamp(System.currentTimeMillis()))
                .build();
    }
}