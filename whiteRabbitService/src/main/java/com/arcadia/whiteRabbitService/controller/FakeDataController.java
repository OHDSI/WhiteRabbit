package com.arcadia.whiteRabbitService.controller;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.dto.FileSettingsDto;
import com.arcadia.whiteRabbitService.dto.ProgressNotificationDto;
import com.arcadia.whiteRabbitService.service.FakeTasksHandler;
import com.arcadia.whiteRabbitService.service.StorageService;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import com.arcadia.whiteRabbitService.service.error.DbTypeNotSupportedException;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import com.arcadia.whiteRabbitService.service.log.WebSocketLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.annotation.SendToUser;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.util.concurrent.Future;

import static com.arcadia.whiteRabbitService.service.log.ProgressNotificationStatus.FAILED;
import static com.arcadia.whiteRabbitService.util.FileUtil.createDirectory;
import static java.lang.String.format;

@RestController
@RequestMapping("/api/fake-data")
@AllArgsConstructor
public class FakeDataController {

    private final FakeTasksHandler fakeTasksHandler;

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

    @MessageExceptionHandler
    @SendToUser("/queue/reply")
    public ProgressNotificationDto handleException(Exception exception) {
        return new ProgressNotificationDto(exception.getMessage(), FAILED);
    }
}
