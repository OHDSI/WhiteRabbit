package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataLog;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataSettings;
import com.arcadia.whiteRabbitService.repository.FakeDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.FakeDataLogRepository;
import com.arcadia.whiteRabbitService.service.error.ServerErrorException;
import com.arcadia.whiteRabbitService.service.request.FakeDataRequest;
import com.arcadia.whiteRabbitService.service.request.ScanReportRequest;
import com.arcadia.whiteRabbitService.service.response.ConversionWithLogsResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

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
public class FakeDataServiceImpl implements FakeDataService {
    private final FakeDataConversionRepository conversionRepository;
    private final FakeDataLogRepository logRepository;
    private final StorageService storageService;
    private final FakeDataConversionService conversionService;
    private final FilesManagerService filesManagerService;

    @Override
    public FakeDataConversion findConversionById(Long conversionId, String username) {
        FakeDataConversion conversion = conversionRepository.findById(conversionId)
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Fake Data Conversion not found by id " + conversionId));
        if (!conversion.getUsername().equals(username)) {
            throw new ResponseStatusException(FORBIDDEN, "Forbidden get other User Fake Data Conversion logs");
        }
        return conversion;
    }

    @Deprecated
    @Override
    public FakeDataConversion generateFakeData(MultipartFile scanReportFile, FakeDataSettings settings, String username) {
        String project = "fake-data";
        String directoryName = format("%s/%s", username, project);
        createDirectory(directoryName);
        settings.setDirectory(directoryName);
        settings.setScanReportFileName(scanReportFile.getName());
        try {
            storageService.store(scanReportFile, settings.getDirectory(), settings.getScanReportFileName());
            FakeDataConversion conversion = FakeDataConversion.builder()
                    .username(username)
                    .project(project)
                    .statusCode(IN_PROGRESS.getCode())
                    .statusName(IN_PROGRESS.getName())
                    .fakeDataSettings(settings)
                    .build();
            settings.setFakeDataConversion(conversion);
            conversionRepository.saveAndFlush(conversion);
            conversionService.runConversion(conversion);

            return conversion;
        } catch (Exception e) {
            log.error("Can not generate fake data {}", e.getMessage());
            settings.destroy();
            throw new ServerErrorException(e.getMessage(), e);
        }
    }

    @Override
    public FakeDataConversion generateFakeData(FakeDataRequest fakeDataRequest, String username) {
        ScanReportRequest scanReportInfo = fakeDataRequest.getScanReportInfo();
        FakeDataSettings settings = fakeDataRequest.getSettings();
        ByteArrayResource scanReportResource = filesManagerService.getFile(scanReportInfo.getDataId());
        String project = "fake-data";
        String directoryName = format("%s/%s", username, project);
        createDirectory(directoryName);
        settings.setDirectory(directoryName);
        settings.setScanReportFileName(scanReportInfo.getFileName());
        try {
            storageService.store(scanReportResource, settings.getDirectory(), settings.getScanReportFileName());
            FakeDataConversion conversion = FakeDataConversion.builder()
                    .username(username)
                    .project(project)
                    .statusCode(IN_PROGRESS.getCode())
                    .statusName(IN_PROGRESS.getName())
                    .fakeDataSettings(settings)
                    .build();
            settings.setFakeDataConversion(conversion);
            conversionRepository.saveAndFlush(conversion);
            conversionService.runConversion(conversion);

            return conversion;
        } catch (Exception e) {
            log.error("Can not generate fake data {}", e.getMessage());
            settings.destroy();
            throw new ServerErrorException(e.getMessage(), e);
        }
    }

    @Override
    public ConversionWithLogsResponse conversionInfoWithLogs(Long conversionId, String username) {
        FakeDataConversion conversion = findConversionById(conversionId, username);
        List<FakeDataLog> logs = logRepository.findAllByFakeDataConversionId(conversionId)
                .stream()
                .sorted(Comparator.comparing(FakeDataLog::getId))
                .collect(Collectors.toList());
        conversion.setLogs(logs);

        return toResponseWithLogs(conversion);
    }

    @Transactional
    @Override
    public void abort(Long conversionId, String username) {
        FakeDataConversion conversion = findConversionById(conversionId, username);
        conversion.setStatus(ABORTED);
        conversionRepository.save(conversion);
    }
}
