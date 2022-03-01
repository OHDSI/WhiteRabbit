package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;

import java.io.File;

public interface ScanDataResultService {
    void saveCompletedResult(File scanReportFile, ScanDataConversion conversion);

    void saveFailedResult(ScanDataConversion conversion, String errorMessage);

    void saveAbortedResult(ScanDataConversion conversion);
}
