package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataSettings;
import com.arcadia.whiteRabbitService.service.response.ConversionWithLogsResponse;
import org.springframework.web.multipart.MultipartFile;

public interface FakeDataService {
    FakeDataConversion findConversionById(Long conversionId, String username);

    FakeDataConversion generateFakeData(MultipartFile scanReportFile, FakeDataSettings settings, String username);

    ConversionWithLogsResponse conversionInfoWithLogs(Long conversionId, String username);

    void abort(Long conversionId, String username);
}
