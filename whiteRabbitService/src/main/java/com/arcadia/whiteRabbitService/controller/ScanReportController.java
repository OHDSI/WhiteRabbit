package com.arcadia.whiteRabbitService.controller;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import com.arcadia.whiteRabbitService.dto.FileSettingsDto;
import com.arcadia.whiteRabbitService.dto.ProgressNotificationDto;
import com.arcadia.whiteRabbitService.dto.ResultDto;
import com.arcadia.whiteRabbitService.service.ScanTasksHandler;
import com.arcadia.whiteRabbitService.service.StorageService;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.simp.annotation.SendToUser;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.arcadia.whiteRabbitService.service.log.ProgressNotificationStatus.FAILED;
import static com.arcadia.whiteRabbitService.util.FileUtil.createDirectory;
import static com.arcadia.whiteRabbitService.util.FileUtil.toScanReportFileFullName;
import static java.lang.String.format;
import static org.springframework.http.HttpHeaders.CONTENT_DISPOSITION;

@AllArgsConstructor
@RestController
@RequestMapping("/api/scan-report")
public class ScanReportController {

    private final ScanTasksHandler scanTasksHandler;

    private final StorageService storageService;

    @PostMapping("/db/{userId}")
    public void generate(@RequestBody DbSettingsDto dto, @PathVariable String userId) {
        boolean created = this.scanTasksHandler.createTask(dto, userId);
        if (!created) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Scan Report Generation Process already run");
        }
    }

    @PostMapping("/files/{userId}")
    public void generate(@PathVariable String userId,
                         @RequestParam String settings,
                         @RequestParam List<MultipartFile> files) throws JsonProcessingException {
        createDirectory(userId);
        List<String> fileNames = files.stream()
                .map(file -> {
                    String name = format("%s/%s", userId, file.getOriginalFilename());
                    storageService.store(file, name);
                    return name;
                })
                .collect(Collectors.toList());
        ObjectMapper mapper = new ObjectMapper();
        FileSettingsDto dto = mapper.readValue(settings, FileSettingsDto.class);
        dto.setFileNames(fileNames);
        dto.setFileDirectory(userId);

        boolean created = this.scanTasksHandler.createTask(dto, userId);
        if (!created) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Scan Report Generation Process already run");
        }
    }

    /* Return scan-report server fileName */
    @GetMapping("/result/{userId}")
    public ResultDto result(@PathVariable String userId) {
        Optional<String> fileName = scanTasksHandler.getTaskResult(userId);
        if (fileName.isPresent()) {
            return new ResultDto(fileName.get());
        } else {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Scan Report not found");
        }
    }

    @GetMapping("/abort/{userId}")
    public void abort(@PathVariable String userId) {
        scanTasksHandler.cancelTask(userId);
    }

    @GetMapping("/{fileName}")
    public ResponseEntity<Resource> downloadScanReport(@PathVariable String fileName) {
        try {
            String location = toScanReportFileFullName(fileName);
            Resource resource = storageService.loadAsResource(location);
            return ResponseEntity
                    .ok()
                    .contentType(MediaType.parseMediaType("application/x-xls"))
                    .header(CONTENT_DISPOSITION, format("attachment; filename=\"%S\"", resource.getFilename()))
                    .body(resource);
        } catch (FileNotFoundException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Scan Report storage period has expired");
        }
    }

    @MessageExceptionHandler
    @SendToUser("/queue/reply")
    public ProgressNotificationDto handleException(Exception exception) {
        return new ProgressNotificationDto(exception.getMessage(), FAILED);
    }
}
