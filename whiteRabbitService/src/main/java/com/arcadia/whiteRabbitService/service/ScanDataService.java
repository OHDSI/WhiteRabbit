package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.*;
import com.arcadia.whiteRabbitService.service.response.ConversionWithLogsResponse;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

public interface ScanDataService {
    ScanDataConversion findConversionById(Long conversionId, String username);

    ScanDataConversion scanDatabaseData(ScanDbSettings settings,
                                        String username);

    ScanDataConversion scanFilesData(ScanFilesSettings setting,
                                     List<MultipartFile> files,
                                     String username);

    ConversionWithLogsResponse conversionInfoWithLogs(Long conversionId, String username);

    void abort(Long conversionId, String username);

    ScanDataResult result(Long conversionId, String username);
}
