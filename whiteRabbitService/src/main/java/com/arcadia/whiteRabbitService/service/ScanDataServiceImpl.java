package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.*;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataLogRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataResultRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.stream.Collectors;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.ABORTED;
import static com.arcadia.whiteRabbitService.model.ConversionStatus.IN_PROGRESS;
import static com.arcadia.whiteRabbitService.util.FileUtil.createDirectory;
import static java.lang.String.format;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@RequiredArgsConstructor
public class ScanDataServiceImpl implements ScanDataService {
    private final ScanDataConversionRepository conversionRepository;
    private final ScanDataConversionService conversionService;
    private final StorageService storageService;
    private final ScanDataLogRepository logRepository;
    private final ScanDataResultRepository resultRepository;

    @Transactional
    @Override
    public ScanDataConversion findConversionById(Long conversionId, String username) {
        ScanDataConversion conversion = conversionRepository.findById(conversionId)
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Conversion not found by id " + conversionId));
        if (!conversion.getUsername().equals(username)) {
            throw new ResponseStatusException(FORBIDDEN, "Forbidden get other User conversion logs");
        }
        return conversion;
    }

    @Transactional
    public ScanDataConversion scanDatabaseData(ScanDbSetting settings,
                                               String username) {
        String project = settings.getProject();
        ScanDataConversion conversion = ScanDataConversion.builder()
                .username(username)
                .dbSettings(settings)
                .project(project)
                .statusCode(IN_PROGRESS.getCode())
                .statusName(IN_PROGRESS.getName())
                .build();
        conversionRepository.saveAndFlush(conversion);
        conversionService.runConversion(conversion);

        return conversion;
    }

    @Transactional
    @Override
    public ScanDataConversion scanFilesData(ScanFilesSettings settings,
                                            List<MultipartFile> files,
                                            String username) {
        String project = settings.getProject();
        String directory = format("%s/%s", username, project);
        createDirectory(directory);
        List<String> fileNames = files.stream()
                .map(file -> storageService.store(file, directory, file.getOriginalFilename()))
                .collect(Collectors.toList());
        settings.setDirectory(directory);
        settings.setFileNames(fileNames);

        ScanDataConversion conversion = ScanDataConversion.builder()
                .username(username)
                .filesSettings(settings)
                .project(project)
                .build();
        conversionRepository.saveAndFlush(conversion);
        conversionService.runConversion(conversion);

        return conversion;
    }

    @Override
    public List<ScanDataLog> logs(Long conversionId, String username) {
        ScanDataConversion conversion = findConversionById(conversionId, username);
        return logRepository.findAllByScanDataConversionId(conversion.getId());
    }

    @Transactional
    @Override
    public void abort(Long conversionId, String username) {
        ScanDataConversion conversion = findConversionById(conversionId, username);
        conversion.setStatus(ABORTED);
    }

    @Override
    public ScanDataResult result(Long conversionId, String username) {
        ScanDataConversion conversion = findConversionById(conversionId, username);
        return resultRepository.findByScanDataConversionId(conversion.getId());
    }
}
