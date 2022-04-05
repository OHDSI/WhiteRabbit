package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.LogStatus;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataLog;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataResult;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataResultRepository;
import com.arcadia.whiteRabbitService.service.request.FileSaveRequest;
import com.arcadia.whiteRabbitService.service.response.FileSaveResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.sql.Timestamp;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.COMPLETED;
import static com.arcadia.whiteRabbitService.model.ConversionStatus.FAILED;
import static com.arcadia.whiteRabbitService.model.LogStatus.ERROR;
import static com.arcadia.whiteRabbitService.model.LogStatus.INFO;

@Service
@RequiredArgsConstructor
public class ScanDataResultServiceImpl implements ScanDataResultService {
    public static final String DATA_KEY = "scan-report";
    
    private final ScanDataConversionRepository conversionRepository;
    private final ScanDataResultRepository resultRepository;
    private final ScanDataLogRepository logRepository;
    private final FilesManagerService filesManagerService;

    @Transactional
    @Override
    public void saveCompletedResult(File scanReportFile, Long conversionId) {
        ScanDataConversion conversion = finsConversionById(conversionId);
        FileSystemResource scanReportResource = new FileSystemResource(scanReportFile);
        FileSaveRequest fileSaveRequest = new FileSaveRequest(
                conversion.getUsername(),
                DATA_KEY,
                scanReportResource
        );
        FileSaveResponse fileSaveResponse = filesManagerService.saveFile(fileSaveRequest);
        ScanDataLog log = createLastLog("Scan report file successfully saved", INFO, conversion);
        logRepository.save(log);
        ScanDataResult result = ScanDataResult.builder()
                .fileName(conversion.getSettings().scanReportFileName())
                .fileId(fileSaveResponse.getId())
                .scanDataConversion(conversion)
                .time(new Timestamp(System.currentTimeMillis()))
                .build();
        conversion.setStatus(COMPLETED);
        conversion.setResult(result);
        resultRepository.save(result);
        conversionRepository.save(conversion);
    }

    @Transactional
    @Override
    public void saveFailedResult(Long conversionId, String errorMessage) {
        ScanDataConversion conversion = finsConversionById(conversionId);
        ScanDataLog log = createLastLog("Failed to scan data: " + errorMessage, ERROR, conversion);
        logRepository.save(log);
        conversion.setStatus(FAILED);
        conversionRepository.save(conversion);
    }

    private ScanDataConversion finsConversionById(Long conversionId) {
        return conversionRepository.findById(conversionId)
                .orElseThrow(() -> new RuntimeException("Scan Data Conversion not found by id " + conversionId));
    }

    private ScanDataLog createLastLog(String message, LogStatus status, ScanDataConversion conversion) {
        return ScanDataLog.builder()
                .message(message)
                .statusCode(status.getCode())
                .statusName(status.getName())
                .time(new Timestamp(System.currentTimeMillis()))
                .percent(100)
                .scanDataConversion(conversion)
                .build();
    }
}
