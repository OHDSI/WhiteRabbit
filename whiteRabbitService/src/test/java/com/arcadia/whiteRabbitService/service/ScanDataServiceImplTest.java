package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDbSetting;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataResultRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.arcadia.whiteRabbitService.service.DbSettingsAdapterTest.createTestDbSettings;
import static com.arcadia.whiteRabbitService.service.FilesManagerServiceTest.readFileFromResources;

@ExtendWith(SpringExtension.class)
class ScanDataServiceImplTest {
    @MockBean
    ScanDataConversionRepository conversionRepository;

    @MockBean
    ScanDataConversionService conversionService;

    @MockBean
    StorageService storageService;

    @MockBean
    ScanDataLogRepository logRepository;

    @MockBean
    ScanDataResultRepository resultRepository;

    ScanDataService scanDataService;

    @BeforeEach
    void setUp() {
        scanDataService = new ScanDataServiceImpl(
                conversionRepository,
                conversionService,
                storageService,
                logRepository,
                resultRepository
        );
    }

    @Test
    void scanDatabaseData() throws InterruptedException {
        ScanDbSetting settings = createTestDbSettings("postgresql", 5432);
        String username = "Perseus";
        String project = "Test";

//        File testScanReport = readFileFromResources(getClass(), "cprd_1k.etl");
//        ExecutorService executor = Executors.newSingleThreadExecutor();
//        Mockito.when(conversionService.runConversion(Mockito.any()))
//                .thenReturn(executor.submit(() -> {
//                    Thread.sleep(100);
//                    resultService.saveCompletedResult(testScanReport, Mockito.any());
//                    return null;
//                }));
    }
}