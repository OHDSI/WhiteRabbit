package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.LogStatus;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataLog;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataResultRepository;
import com.arcadia.whiteRabbitService.service.response.ConversionWithLogsResponse;
import com.arcadia.whiteRabbitService.util.LogUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.sql.Timestamp;
import java.util.List;

import static com.arcadia.whiteRabbitService.model.LogStatus.INFO;
import static com.arcadia.whiteRabbitService.service.ScanDataConversionServiceTest.createScanDataConversion;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ExtendWith(SpringExtension.class)
public class ScanDataServiceTest {
    @Autowired
    ScanDataConversionRepository conversionRepository;

    @MockBean
    ScanDataConversionService conversionService;

    @MockBean
    StorageService storageService;

    @Autowired
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