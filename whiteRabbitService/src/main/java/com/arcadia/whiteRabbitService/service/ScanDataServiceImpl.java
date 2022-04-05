package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.*;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataResultRepository;
import com.arcadia.whiteRabbitService.service.error.ServerErrorException;
import com.arcadia.whiteRabbitService.service.response.ConversionWithLogsResponse;
import com.arcadia.whiteRabbitService.util.ConversionUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.ABORTED;
import static com.arcadia.whiteRabbitService.model.ConversionStatus.IN_PROGRESS;
import static com.arcadia.whiteRabbitService.util.ConversionUtil.toResponseWithLogs;
import static com.arcadia.whiteRabbitService.util.FileUtil.createDirectory;
import static java.lang.String.format;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@RequiredArgsConstructor
@Slf4j
public class ScanDataServiceImpl implements ScanDataService {
    private final ScanDataConversionRepository conversionRepository;
    private final ScanDataConversionService conversionService;
    private final StorageService storageService;
    private final ScanDataLogRepository logRepository;
    private final ScanDataResultRepository resultRepository;
    
    @Override
    public ScanDataConversion findConversionById(Long conversionId, String username) {
        ScanDataConversion conversion = conversionRepository.findById(conversionId)
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Scan Data Conversion not found by id " + conversionId));
        if (!conversion.getUsername().equals(username)) {
            throw new ResponseStatusException(FORBIDDEN, "Forbidden to get Scan Data Conversion for other user");
        }
        return conversion;
    }

    @Transactional
    public ScanDataConversion scanDatabaseData(ScanDbSettings settings,
                                               String username) {
        String project = settings.getDatabase();
        ScanDataConversion conversion = ScanDataConversion.builder()
                .username(username)
                .project(project)
                .statusCode(IN_PROGRESS.getCode())
                .statusName(IN_PROGRESS.getName())
                .dbSettings(settings)
                .build();
        settings.setScanDataConversion(conversion);
        conversionRepository.saveAndFlush(conversion);
        conversionService.runConversion(conversion);

        return conversion;
    }

    @Transactional
    @Override
    public ScanDataConversion scanFilesData(ScanFilesSettings settings,
                                            List<MultipartFile> files,
                                            String username) {
        String project = "csv";
        String directoryName = format("%s/%s", username, project);
        createDirectory(directoryName);
        settings.setDirectory(directoryName);
        try {
            List<String> fileNames = new ArrayList<>();
            for (MultipartFile file : files) {
                String store = storageService.store(file, directoryName, file.getOriginalFilename());
                fileNames.add(store);
            }
            settings.setFileNames(fileNames);

            ScanDataConversion conversion = ScanDataConversion.builder()
                    .username(username)
                    .project(project)
                    .statusCode(IN_PROGRESS.getCode())
                    .statusName(IN_PROGRESS.getName())
                    .filesSettings(settings)
                    .build();
            settings.setScanDataConversion(conversion);
            conversionRepository.saveAndFlush(conversion);
            conversionService.runConversion(conversion);

            return conversion;
        } catch (Exception e) {
            log.error("Can not scan files data {}", e.getMessage());
            settings.destroy();
            throw new ServerErrorException(e.getMessage(), e);
        }
    }

    @Override
    public ConversionWithLogsResponse conversionInfoWithLogs(Long conversionId, String username) {
        ScanDataConversion conversion = findConversionById(conversionId, username);
        List<ScanDataLog> logs = logRepository.findAllByScanDataConversionId(conversion.getId())
                .stream()
                .sorted(Comparator.comparing(ScanDataLog::getId))
                .collect(Collectors.toList());
        conversion.setLogs(logs);

        return toResponseWithLogs(conversion);
    }

    @Transactional
    @Override
    public void abort(Long conversionId, String username) {
        ScanDataConversion conversion = findConversionById(conversionId, username);
        conversion.setStatus(ABORTED);
        conversionRepository.save(conversion);
    }

    @Override
    public ScanDataResult result(Long conversionId, String username) {
        ScanDataConversion conversion = findConversionById(conversionId, username);
        return resultRepository.findByScanDataConversionId(conversion.getId())
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Scan Data Conversion Result not found by conversion id " + conversionId));
    }
}
