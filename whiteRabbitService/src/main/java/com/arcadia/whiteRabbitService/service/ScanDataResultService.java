package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;

import java.io.File;

public interface ScanDataResultService {
    void saveCompletedResult(File scanReportFile, Long conversionId);

    void saveFailedResult(Long conversionId, String errorMessage);
}
