package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.LogStatus;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataLog;
import com.arcadia.whiteRabbitService.repository.FakeDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.FakeDataLogRepository;
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
import static com.arcadia.whiteRabbitService.service.FakeDataResultServiceImplTest.createFakeDataConversion;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ExtendWith(SpringExtension.class)
public class FakeDataServiceTest {
    @Autowired
    FakeDataConversionRepository conversionRepository;

    @Autowired
    FakeDataLogRepository logRepository;

    @MockBean
    StorageService storageService;

    @MockBean
    FakeDataConversionService conversionService;

    @MockBean
    FilesManagerService filesManagerService;

    FakeDataService fakeDataService;

    @BeforeEach
    void setUp() {
        fakeDataService = new FakeDataServiceImpl(
                conversionRepository,
                logRepository,
                storageService,
                conversionService,
                filesManagerService
        );
    }

    @Test
    void conversionInfoWithLogs() {
        FakeDataConversion conversion = createFakeDataConversion();
        conversionRepository.save(conversion);
        FakeDataLog log1 = createFakeDataLog(conversion, "Test", INFO, 1);
        FakeDataLog log2 = createFakeDataLog(conversion, "Test", INFO, 2);
        FakeDataLog log3 = createFakeDataLog(conversion, "Test", INFO, 3);
        List<FakeDataLog> logs = List.of(log1, log2, log3);
        logRepository.saveAll(logs);

        ConversionWithLogsResponse response =
                fakeDataService.conversionInfoWithLogs(conversion.getId(), conversion.getUsername());

        assertEquals(conversion.getId(), response.getId());
        assertArrayEquals(logs.stream().map(LogUtil::toResponse).toArray(), response.getLogs().toArray());
    }

    public static FakeDataLog createFakeDataLog(FakeDataConversion conversion,
                                                String message,
                                                LogStatus status,
                                                Integer percent) {
        return FakeDataLog.builder()
                .fakeDataConversion(conversion)
                .message(message)
                .statusCode(status.getCode())
                .statusName(status.getName())
                .percent(percent)
                .time(new Timestamp(System.currentTimeMillis()))
                .build();
    }
}
