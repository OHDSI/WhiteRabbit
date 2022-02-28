package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataSettings;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import com.arcadia.whiteRabbitService.util.DatabaseInterrupter;
import com.arcadia.whiteRabbitService.util.DatabaseLogger;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.concurrent.Future;

@Service
@RequiredArgsConstructor
public class ScanDataConversionServiceImpl implements ScanDataConversionService {
    private final ScanDataLogRepository logRepository;
    private final ScanDataConversionRepository conversionRepository;
    private final WhiteRabbitFacade whiteRabbitFacade;
    private final ScanDataResultService resultService;

    @Async
    @Override
    public Future<Void> runConversion(ScanDataConversion conversion) {
        DatabaseLogger logger = new DatabaseLogger(logRepository, conversion);
        DatabaseInterrupter interrupter = new DatabaseInterrupter(conversionRepository, conversion.getId());
        ScanDataSettings settings = conversion.getSettings();
        try {
            File scanReportFile = whiteRabbitFacade.generateScanReport(settings, logger, interrupter);
            resultService.saveCompletedResult(scanReportFile, conversion);
            scanReportFile.delete();
        } catch (InterruptedException e) {
            resultService.saveAbortedResult(conversion);
            throw new FailedToScanException(e.getMessage());
        } catch (Exception e) {
            resultService.saveFailedResult(conversion, logger, e.getMessage());
            throw new FailedToScanException(e.getMessage());
        } finally {
            settings.destroy();
        }
        return new AsyncResult<>(null);
    }
}
