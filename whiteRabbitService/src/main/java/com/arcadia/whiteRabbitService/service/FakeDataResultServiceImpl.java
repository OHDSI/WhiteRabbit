package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.repository.FakeDataConversionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.*;

@Service
@RequiredArgsConstructor
public class FakeDataResultServiceImpl implements FakeDataResultService {
    private final FakeDataConversionRepository conversionRepository;

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
    public void saveFailedResult(Long conversionId) {
        FakeDataConversion conversion = conversionRepository.findById(conversionId)
                .orElseThrow(() -> new RuntimeException("Fake Data Conversion not found by id " + conversionId));
        conversion.setStatus(FAILED);
        conversionRepository.save(conversion);
    }

    @Transactional
    @Override
    public void saveAbortedResult(Long conversionId) {
        FakeDataConversion conversion = conversionRepository.findById(conversionId)
                .orElseThrow(() -> new RuntimeException("Fake Data Conversion not found by id " + conversionId));
        conversion.setStatus(ABORTED);
        conversionRepository.save(conversion);
    }
}
