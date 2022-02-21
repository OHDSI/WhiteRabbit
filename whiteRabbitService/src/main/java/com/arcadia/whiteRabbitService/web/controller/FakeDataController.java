package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.service.FakeDataTasksHandler;
import com.arcadia.whiteRabbitService.service.StorageService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import static com.arcadia.whiteRabbitService.util.FileUtil.createDirectory;
import static java.lang.String.format;

@RestController
@RequestMapping("/api/fake-data")
@RequiredArgsConstructor
public class FakeDataController {

    private final FakeDataTasksHandler fakeTasksHandler;

    private final StorageService storageService;

    @PostMapping("/{userId}")
    public void generate(@PathVariable String userId,
                         @RequestParam MultipartFile file,
                         @RequestParam String settings) throws JsonProcessingException {
        createDirectory(userId);
        String scanReportFileName = format("%s/%s", userId, file.getOriginalFilename());
        storageService.store(file, scanReportFileName);

        ObjectMapper mapper = new ObjectMapper();
        FakeDataParamsDto dto = mapper.readValue(settings, FakeDataParamsDto.class);
        dto.setScanReportFileName(scanReportFileName);
        dto.setDirectory(userId);

        boolean created = this.fakeTasksHandler.createTask(dto, userId);
        if (!created) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Fake Data Generation Process already run");
        }
    }

    @GetMapping("/{userId}")
    public void abort(@PathVariable String userId) {
        fakeTasksHandler.cancelTask(userId);
    }
}
