package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataLog;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataSettings;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import com.arcadia.whiteRabbitService.service.util.DatabaseLogger;
import com.arcadia.whiteRabbitService.service.util.ScanDataInterrupter;
import com.arcadia.whiteRabbitService.service.util.ScanDataLogCreator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.concurrent.Future;

@Service
@RequiredArgsConstructor
@Slf4j
public class ScanDataConversionServiceImpl implements ScanDataConversionService {
    public static final String FAILED_TO_SCAN_DATA_MESSAGE = "Failed to scan data";

    private final ScanDataLogRepository logRepository;
    private final ScanDataConversionRepository conversionRepository;
    private final WhiteRabbitFacade whiteRabbitFacade;
    private final ScanDataResultService resultService;

    @Async
    @Override
    public Future<Void> runConversion(ScanDataConversion conversion) {
        ScanDataSettings settings = conversion.getSettings();
        ScanDataLogCreator logCreator = new ScanDataLogCreator(conversion);
        DatabaseLogger<ScanDataLog> logger = new DatabaseLogger<>(logRepository, logCreator);
        ScanDataInterrupter interrupter = new ScanDataInterrupter(conversionRepository, conversion.getId());
        try {
            File scanReportFile = whiteRabbitFacade.generateScanReport(settings, logger, interrupter);
            resultService.saveCompletedResult(scanReportFile, conversion.getId());
            scanReportFile.delete();
        } catch (InterruptedException e) {
            log.warn(e.getMessage());
            resultService.saveAbortedResult(conversion.getId());
        } catch (Exception e) {
            try {
                logger.error(e.getMessage());
                logger.error(FAILED_TO_SCAN_DATA_MESSAGE);
            } catch (Exception logException) {
                log.error("Can not log message. " + logException.getMessage());
            }
            log.error(e.getMessage());
            resultService.saveFailedResult(conversion.getId());
        } finally {
            settings.destroy();
        }
        return new AsyncResult<>(null);
    }
}
