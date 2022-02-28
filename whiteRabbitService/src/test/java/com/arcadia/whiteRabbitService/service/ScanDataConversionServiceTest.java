package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.IN_PROGRESS;
import static com.arcadia.whiteRabbitService.service.DbSettingsAdapterTest.createTestDbSettings;
import static com.arcadia.whiteRabbitService.service.FilesManagerServiceTest.readFileFromResources;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
class ScanDataConversionServiceTest {
    @MockBean
    ScanDataLogRepository logRepository;

    @MockBean
    ScanDataConversionRepository conversionRepository;

    @MockBean
    WhiteRabbitFacade whiteRabbitFacade;

    @MockBean
    ScanDataResultService resultService;

    @MockBean
    File scanReportFile;

    ScanDataConversionService conversionService;

    @BeforeEach
    void setUp() {
        conversionService = new ScanDataConversionServiceImpl(
                logRepository,
                conversionRepository,
                whiteRabbitFacade,
                resultService
        );
    }

    @Test
    void successfullyRunConversion() throws InterruptedException {
        ScanDataConversion conversion = createConversion();
        when(whiteRabbitFacade.generateScanReport(eq(conversion.getSettings()), any(), any()))
                .thenReturn(scanReportFile);
        conversionService.runConversion(conversion);

        Mockito.verify(resultService).saveCompletedResult(scanReportFile, conversion);
    }

    @Test
    void saveAbortedResult() throws InterruptedException {
        ScanDataConversion conversion = createConversion();
        when(whiteRabbitFacade.generateScanReport(eq(conversion.getSettings()), any(), any()))
                .thenThrow(new InterruptedException());
        assertThrows(FailedToScanException.class, () -> conversionService.runConversion(conversion));
        Mockito.verify(resultService).saveAbortedResult(conversion);
    }

    @Test
    void saveFailedResult() throws InterruptedException {
        ScanDataConversion conversion = createConversion();
        when(whiteRabbitFacade.generateScanReport(eq(conversion.getSettings()), any(), any()))
                .thenThrow(new RuntimeException());
        assertThrows(FailedToScanException.class, () -> conversionService.runConversion(conversion));
        Mockito.verify(resultService).saveFailedResult(eq(conversion), any(), any());
    }

    public static ScanDataConversion createConversion() {
        return ScanDataConversion.builder()
                .id(1L)
                .username("Perseus")
                .project("Test")
                .statusCode(IN_PROGRESS.getCode())
                .statusName(IN_PROGRESS.getName())
                .dbSettings(createTestDbSettings("postgresql", 5433))
                .build();
    }
}