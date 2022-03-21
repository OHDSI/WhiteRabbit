package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.LogStatus;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataLog;
import com.arcadia.whiteRabbitService.repository.FakeDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.FakeDataLogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.COMPLETED;
import static com.arcadia.whiteRabbitService.model.ConversionStatus.FAILED;
import static com.arcadia.whiteRabbitService.model.LogStatus.ERROR;

@Service
@RequiredArgsConstructor
public class FakeDataResultServiceImpl implements FakeDataResultService {
    private final FakeDataConversionRepository conversionRepository;
    private final FakeDataLogRepository logRepository;

    @Transactional
    @Override
    public void saveCompletedResult(Long conversionId) {
        FakeDataConversion conversion = conversionRepository.findById(conversionId)
                .orElseThrow(() -> new RuntimeException("Fake Data Conversion not found by id " + conversionId));
        conversion.setStatus(COMPLETED);
        conversionRepository.save(conversion);
    }

    @Transactional
    @Override
    public void saveFailedResult(Long conversionId, String errorMessage) {
        FakeDataConversion conversion = conversionRepository.findById(conversionId)
                .orElseThrow(() -> new RuntimeException("Fake Data Conversion not found by id " + conversionId));
        FakeDataLog log = createLastLog("Failed to generate Fake Data: " + errorMessage, ERROR, conversion);
        logRepository.save(log);
        conversion.setStatus(FAILED);
        conversionRepository.save(conversion);
    }

    private FakeDataLog createLastLog(String message, LogStatus status, FakeDataConversion conversion) {
        return FakeDataLog.builder()
                .message(message)
                .statusCode(status.getCode())
                .statusName(status.getName())
                .time(new Timestamp(System.currentTimeMillis()))
                .percent(100)
                .fakeDataConversion(conversion)
                .build();
    }
}
