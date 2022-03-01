package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataSettings;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import com.arcadia.whiteRabbitService.service.util.DatabaseInterrupter;
import com.arcadia.whiteRabbitService.service.util.DatabaseLogger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.concurrent.Future;

import static com.arcadia.whiteRabbitService.service.ScanDataResultServiceImpl.FAILED_TO_SCAN_MESSAGE;

@Service
@RequiredArgsConstructor
@Slf4j
public class ScanDataConversionServiceImpl implements ScanDataConversionService {
    private final ScanDataLogRepository logRepository;
    private final ScanDataConversionRepository conversionRepository;
    private final WhiteRabbitFacade whiteRabbitFacade;
    private final ScanDataResultService resultService;

    @Async
    @Override
    public Future<Void> runConversion(ScanDataConversion conversion) {
        ScanDataSettings settings = conversion.getSettings();
        DatabaseLogger logger = new DatabaseLogger(logRepository, conversion);
        DatabaseInterrupter interrupter = new DatabaseInterrupter(conversionRepository, conversion.getId());
        try {
            log.info("Started scan data process");
            File scanReportFile = whiteRabbitFacade.generateScanReport(settings, logger, interrupter);
            log.info("Finished scan data process");
            resultService.saveCompletedResult(scanReportFile, conversion);
            scanReportFile.delete();
        } catch (InterruptedException e) {
            logger.info("Scan data process aborted by User");
            resultService.saveAbortedResult(conversion);
            throw new FailedToScanException(e.getMessage());
        } catch (Exception e) {
            try {
                logger.error(e.getMessage());
                logger.error(FAILED_TO_SCAN_MESSAGE);
            } catch (Exception logException) {
                log.error("Can not log scan data error message: " + logException.getMessage());
            }
            resultService.saveFailedResult(conversion, e.getMessage());
            throw new FailedToScanException(e.getMessage());
        } finally {
            settings.destroy();
        }
        return new AsyncResult<>(null);
    }
}
