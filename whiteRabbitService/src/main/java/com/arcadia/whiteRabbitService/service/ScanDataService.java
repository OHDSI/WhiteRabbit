package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

public interface ScanDataService {
    ScanDataConversion findConversionById(Long conversionId, String username);

    ScanDataConversion scanDatabaseData(ScanDbSetting settings,
                                        String username);

    ScanDataConversion scanFilesData(ScanFilesSettings setting,
                                     List<MultipartFile> files,
                                     String username);

    List<ScanDataLog> logs(Long conversionId, String username);

    void abort(Long conversionId, String username);

    ScanDataResult result(Long conversionId, String username);
}
