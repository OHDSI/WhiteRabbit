package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataResult;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataResultRepository;
import com.arcadia.whiteRabbitService.service.request.FileSaveRequest;
import com.arcadia.whiteRabbitService.service.response.FileSaveResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.sql.Timestamp;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.*;

@Service
@RequiredArgsConstructor
public class ScanDataResultServiceImpl implements ScanDataResultService {
    public static final String DATA_KEY = "white-rabbit";
    public static final String FAILED_TO_SCAN_MESSAGE = "Failed to scan";

    private final ScanDataResultRepository resultRepository;
    private final ScanDataConversionRepository conversionRepository;
    private final FilesManagerService filesManagerService;

    @Transactional
    @Override
    public void saveCompletedResult(File scanReportFile, ScanDataConversion conversion) {
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
        conversion.setStatus(COMPLETED);
        conversion.setResult(result);
        conversionRepository.save(conversion);
    }

    @Transactional
    @Override
    public void saveFailedResult(ScanDataConversion conversion, String errorMessage) {
        conversion.setStatus(FAILED);
        conversionRepository.save(conversion);
    }

    @Transactional
    @Override
    public void saveAbortedResult(ScanDataConversion conversion) {
        conversion.setStatus(ABORTED);
        conversionRepository.save(conversion);
    }
}
