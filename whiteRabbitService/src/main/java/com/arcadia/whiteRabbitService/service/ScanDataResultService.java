package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.util.DatabaseLogger;
import org.ohdsi.utilities.Logger;

import java.io.File;

public interface ScanDataResultService {
    void saveCompletedResult(File scanReportFile, ScanDataConversion conversion);

    void saveFailedResult(ScanDataConversion conversion, Logger logger, String errorMessage);

    void saveAbortedResult(ScanDataConversion conversion);
}
