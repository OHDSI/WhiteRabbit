package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDbSettings;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.IN_PROGRESS;
import static com.arcadia.whiteRabbitService.service.DbSettingsAdapterTest.createTestDbSettings;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
public class ScanDataConversionServiceTest {
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
        ScanDataConversion conversion = createScanDataConversion();
        when(whiteRabbitFacade.generateScanReport(eq(conversion.getSettings()), any(), any()))
                .thenReturn(scanReportFile);
        conversionService.runConversion(conversion);

        Mockito.verify(resultService).saveCompletedResult(scanReportFile, conversion.getId());
    }

    @Test
    void saveFailedResult() throws InterruptedException {
        ScanDataConversion conversion = createScanDataConversion();
        RuntimeException exception = new RuntimeException("Test error");
        when(whiteRabbitFacade.generateScanReport(eq(conversion.getSettings()), any(), any()))
                .thenThrow(exception);
        conversionService.runConversion(conversion);
        Mockito.verify(resultService).saveFailedResult(eq(conversion.getId()), eq(exception.getMessage()));
    }

    public static ScanDataConversion createScanDataConversion() {
        ScanDbSettings settings = createTestDbSettings("postgresql", 5433);
        ScanDataConversion conversion = ScanDataConversion.builder()
                .username("Perseus")
                .project("Test")
                .statusCode(IN_PROGRESS.getCode())
                .statusName(IN_PROGRESS.getName())
                .dbSettings(settings)
                .build();
        settings.setScanDataConversion(conversion);

        return conversion;
    }
}