package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.model.scandata.*;
import com.arcadia.whiteRabbitService.service.FilesManagerService;
import com.arcadia.whiteRabbitService.service.ScanDataService;
import com.arcadia.whiteRabbitService.service.error.BadRequestException;
import com.arcadia.whiteRabbitService.service.response.ConversionWithLogsResponse;
import com.arcadia.whiteRabbitService.service.response.ScanReportResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

import static java.lang.String.format;
import static org.springframework.http.HttpHeaders.CONTENT_DISPOSITION;
import static org.springframework.http.ResponseEntity.noContent;
import static org.springframework.http.ResponseEntity.ok;

@RestController
@RequestMapping("/api/scan-report")
@RequiredArgsConstructor
@Slf4j
public class ScanDataController {
    public static final String INCORRECT_PARAMS_MESSAGE = "Incorrect Scan Data Params";

    private final ScanDataService scanDataService;
    private final FilesManagerService filesManagerService;

    @PostMapping("/db")
    public ResponseEntity<ScanDataConversion> generate(@RequestHeader("Username") String username,
                                                       @Validated @RequestBody ScanDbSettings dbSetting) {
        log.info("Rest request to generate scan report by database settings");
        return ok(scanDataService.scanDatabaseData(dbSetting, username));
    }

    @PostMapping("/files")
    public ResponseEntity<ScanDataConversion> generate(@RequestHeader("Username") String username,
                                                       @RequestParam String settings,
                                                       @RequestParam List<MultipartFile> files) {
        log.info("Rest request to generate scan report by files settings");
        try {
            ObjectMapper mapper = new ObjectMapper();
            ScanFilesSettings scanFilesSettings = mapper.readValue(settings, ScanFilesSettings.class);
            return ok(scanDataService.scanFilesData(scanFilesSettings, files, username));
        } catch (JsonProcessingException e) {
            log.error(INCORRECT_PARAMS_MESSAGE + ". " + e.getMessage());
            throw new BadRequestException(INCORRECT_PARAMS_MESSAGE);
        }
    }

    @GetMapping("/abort/{conversionId}")
    public ResponseEntity<Void> abort(@RequestHeader("Username") String username,
                                      @PathVariable Long conversionId) {
        log.info("Rest request to abort Scan Data conversion with id {}", conversionId);
        scanDataService.abort(conversionId, username);
        return noContent().build();
    }

    @GetMapping("/conversion/{conversionId}")
    public ResponseEntity<ConversionWithLogsResponse> conversionInfoAndLogs(@RequestHeader("Username") String username,
                                                                            @PathVariable Long conversionId) {
        log.info("Rest request to get Scan Data conversion info by id {}", conversionId);
        return ok(scanDataService.conversionInfoWithLogs(conversionId, username));
    }

    @GetMapping("/result/{conversionId}")
    public ResponseEntity<ScanReportResponse> scanResult(@RequestHeader("Username") String username,
                                                         @PathVariable Long conversionId) {
        ScanDataResult result = scanDataService.result(conversionId, username);
        ScanReportResponse response = new ScanReportResponse(result.getFileId(), result.getFileName());
        return ok(response);
    }

    @GetMapping("/result-as-resource/{conversionId}")
    public ResponseEntity<Resource> downloadScanReport(@RequestHeader("Username") String username,
                                                       @PathVariable Long conversionId) {
        log.info("Rest request to download scan report with conversion id {}", conversionId);
        ScanDataResult result = scanDataService.result(conversionId, username);
        Resource resource = filesManagerService.getFile(result.getFileId());
        return ok()
                .contentType(MediaType.parseMediaType("application/x-xls"))
                .header(CONTENT_DISPOSITION, format("attachment; filename=\"%S\"", result.getFileName()))
                .body(resource);
    }
}
