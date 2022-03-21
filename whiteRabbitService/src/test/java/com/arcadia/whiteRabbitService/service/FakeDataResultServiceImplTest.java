package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataSettings;
import com.arcadia.whiteRabbitService.repository.FakeDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.FakeDataLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.*;
import static com.arcadia.whiteRabbitService.model.ConversionStatus.FAILED;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ExtendWith(SpringExtension.class)
public class FakeDataResultServiceImplTest {
    @Autowired
    FakeDataConversionRepository conversionRepository;

    @Autowired
    FakeDataLogRepository logRepository;

    FakeDataResultService fakeDataResultService;

    @BeforeEach
    void setUp() {
        fakeDataResultService = new FakeDataResultServiceImpl(conversionRepository, logRepository);
    }

    @Test
    void saveCompletedResult() {
        FakeDataConversion conversion = createFakeDataConversion();
        conversionRepository.save(conversion);
        fakeDataResultService.saveCompletedResult(conversion.getId());
        conversion = conversionRepository.findById(conversion.getId()).get();

        assertEquals(COMPLETED.getCode(), conversion.getStatusCode());
        assertEquals(COMPLETED.getName(), conversion.getStatusName());
    }

    @Test
    void saveFailedResult() {
        FakeDataConversion conversion = createFakeDataConversion();
        conversionRepository.save(conversion);
        fakeDataResultService.saveFailedResult(conversion.getId(), "Test error");
        conversion = conversionRepository.findById(conversion.getId()).get();

        assertEquals(FAILED.getCode(), conversion.getStatusCode());
        assertEquals(FAILED.getName(), conversion.getStatusName());
    }

    public static FakeDataConversion createFakeDataConversion() {
        FakeDataSettings fakeDataSettings = createTestFakeDataSettings();
        FakeDataConversion conversion = FakeDataConversion.builder()
                .username("Perseus")
                .project("Test")
                .statusCode(IN_PROGRESS.getCode())
                .statusName(IN_PROGRESS.getName())
                .fakeDataSettings(fakeDataSettings)
                .build();
        fakeDataSettings.setFakeDataConversion(conversion);

        return conversion;
    }

    public static FakeDataSettings createTestFakeDataSettings() {
        return FakeDataSettings.builder()
                .maxRowCount(10000)
                .doUniformSampling(false)
                .userSchema("test")
                .scanReportFileName("test.xlsx")
                .directory("test")
                .build();
    }
}