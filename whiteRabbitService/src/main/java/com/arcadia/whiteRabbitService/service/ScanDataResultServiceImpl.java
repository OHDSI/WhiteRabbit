package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataResult;
import com.arcadia.whiteRabbitService.repository.ScanDataResultRepository;
import com.arcadia.whiteRabbitService.service.request.FileSaveRequest;
import com.arcadia.whiteRabbitService.service.response.FileSaveResponse;
import lombok.RequiredArgsConstructor;
import org.ohdsi.utilities.Logger;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.sql.Timestamp;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.*;

@Service
@Transactional
@RequiredArgsConstructor
public class ScanDataResultServiceImpl implements ScanDataResultService {
    public static final String DATA_KEY = "white-rabbit";
    public static final String FAILED_TO_SCAN_MESSAGE = "Failed to scan";

    private final ScanDataResultRepository resultRepository;
    private final FilesManagerService filesManagerService;

    @Override
    public void saveCompletedResult(File scanReportFile, ScanDataConversion conversion) {
        conversion.setStatus(COMPLETED);
        FileSystemResource scanReportResource = new FileSystemResource(scanReportFile);
        FileSaveRequest fileSaveRequest = new FileSaveRequest(
                conversion.getUsername(),
                DATA_KEY,
                scanReportResource
        );
        FileSaveResponse fileSaveResponse = filesManagerService.saveFile(fileSaveRequest);
        ScanDataResult result = ScanDataResult.builder()
                .fileName(conversion.getSettings().scanReportFileName())
                .fileKey(fileSaveResponse.getHash())
                .scanDataConversion(conversion)
                .time(new Timestamp(System.currentTimeMillis()))
                .build();
        resultRepository.save(result);
        conversion.setResult(result);
    }

    @Override
    public void saveFailedResult(ScanDataConversion conversion, Logger logger, String errorMessage) {
        logger.error(errorMessage);
        logger.error(FAILED_TO_SCAN_MESSAGE);
        conversion.setStatus(FAILED);
    }

    @Override
    public void saveAbortedResult(ScanDataConversion conversion) {
        conversion.setStatus(ABORTED);
    }
}
