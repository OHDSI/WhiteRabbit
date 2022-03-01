package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.model.scandata.*;
import com.arcadia.whiteRabbitService.service.FilesManagerService;
import com.arcadia.whiteRabbitService.service.ScanDataService;
import com.arcadia.whiteRabbitService.service.error.BadRequestException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

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
    private final ScanDataService scanDataService;
    private final FilesManagerService filesManagerService;

    @PostMapping("/db")
    public ResponseEntity<ScanDataConversion> generate(@RequestHeader("Username") String username,
                                                       @RequestBody ScanDbSetting dbSetting) {
        log.info("Rest request to generate scan report by database settings {}", dbSetting);
        return ok(scanDataService.scanDatabaseData(dbSetting, username));
    }

    @PostMapping("/files")
    public ResponseEntity<ScanDataConversion> generate(@RequestHeader("Username") String username,
                                                       @RequestParam String fileType,
                                                       @RequestParam String delimiter,
                                                       @RequestParam String scanDataParams,
                                                       @RequestParam List<MultipartFile> files) {
        log.info("Rest request to generate scan report by files settings");
        try {
            ObjectMapper mapper = new ObjectMapper();
            ScanDataParams params = mapper.readValue(scanDataParams, ScanDataParams.class);
            ScanFilesSettings settings = ScanFilesSettings.builder()
                    .fileType(fileType)
                    .delimiter(delimiter)
                    .scanDataParams(params)
                    .build();
            return ok(scanDataService.scanFilesData(settings, files, username));
        } catch (JsonProcessingException e) {
            throw new BadRequestException("Incorrect Scan Data Params");
        }
    }

    @GetMapping("/logs/{conversionId}")
    public ResponseEntity<List<ScanDataLog>> logs(@RequestHeader("Username") String username,
                                                  @PathVariable Long conversionId) {
        log.info("Rest request to get logs for conversion with id {}", conversionId);
        return ok(scanDataService.logs(conversionId, username));
    }

    @GetMapping("/abort/{conversionId}")
    public ResponseEntity<Void> abort(@RequestHeader("Username") String username,
                                      @PathVariable Long conversionId) {
        log.info("Rest request to abort conversion with id {}", conversionId);
        scanDataService.abort(conversionId, username);
        return noContent().build();
    }

    @GetMapping("/conversion/{conversionId}")
    public ResponseEntity<ScanDataConversion> conversionInfo(@RequestHeader("Username") String username,
                                                     @PathVariable Long conversionId) {
        log.info("Rest request to get conversion info by id {}", conversionId);
        return ok(scanDataService.findConversionById(conversionId, username));
    }

    @GetMapping("/result/{conversionId}")
    public ResponseEntity<Resource> downloadScanReport(@RequestHeader("Username") String username,
                                                       @PathVariable Long conversionId) {
        log.info("Rest request to download scan report with conversion id {}", conversionId);
        ScanDataResult result = scanDataService.result(conversionId, username);
        Resource resource = filesManagerService.getFile(result.getFileKey());
        return ok()
                .contentType(MediaType.parseMediaType("application/x-xls"))
                .header(CONTENT_DISPOSITION, format("attachment; filename=\"%S\"", result.getFileName()))
                .body(resource);
    }
}
