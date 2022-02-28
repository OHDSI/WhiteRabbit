package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.model.scandata.*;
import com.arcadia.whiteRabbitService.service.FilesManagerService;
import com.arcadia.whiteRabbitService.service.ScanDataService;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
public class ScanDataController {
    private final ScanDataService scanDataService;
    private final FilesManagerService filesManagerService;

    @PostMapping("/db")
    public ResponseEntity<ScanDataConversion> generate(@RequestHeader("Username") String username,
                                                       @RequestBody ScanDbSetting dbSetting) {
        return ok(scanDataService.scanDatabaseData(dbSetting, username));
    }

    @PostMapping("/files")
    public ResponseEntity<ScanDataConversion> generate(@RequestHeader("Username") String username,
                                                       @RequestParam String fileType,
                                                       @RequestParam String delimiter,
                                                       @RequestParam String project,
                                                       @RequestParam List<MultipartFile> files) {
        ScanFilesSettings settings = ScanFilesSettings.builder()
                .fileType(fileType)
                .delimiter(delimiter)
                .project(project)
                .build();
        return ok(scanDataService.scanFilesData(settings, files, username));
    }

    @GetMapping("/logs/{conversionId}")
    public ResponseEntity<List<ScanDataLog>> logs(@RequestHeader("Username") String username,
                                                  @PathVariable Long conversionId) {
        return ok(scanDataService.logs(conversionId, username));
    }

    @GetMapping("/abort/{conversionId}")
    public ResponseEntity<Void> abort(@RequestHeader("Username") String username,
                                      @PathVariable Long conversionId) {
        scanDataService.abort(conversionId, username);
        return noContent().build();
    }

    @GetMapping("/conversion/{conversionId}")
    public ResponseEntity<ScanDataConversion> result(@RequestHeader("Username") String username,
                                                     @PathVariable Long conversionId) {
        return ok(scanDataService.findConversionById(conversionId, username));
    }

    @GetMapping("/result/{conversionId}")
    public ResponseEntity<Resource> downloadScanReport(@RequestHeader("Username") String username,
                                                       @PathVariable Long conversionId) {
        ScanDataResult result = scanDataService.result(conversionId, username);
        Resource resource = filesManagerService.getFile(result.getFileKey());
        return ok()
                .contentType(MediaType.parseMediaType("application/x-xls"))
                .header(CONTENT_DISPOSITION, format("attachment; filename=\"%S\"", result.getFileName()))
                .body(resource);
    }
}
