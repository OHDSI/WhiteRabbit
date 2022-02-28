package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.service.task.FakeDataTasksHandler;
import com.arcadia.whiteRabbitService.service.StorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import static java.lang.String.format;

@RestController
@RequestMapping("/api/fake-data")
@RequiredArgsConstructor
public class FakeDataController {

    private final FakeDataTasksHandler fakeTasksHandler;

    private final StorageService storageService;

//    @PostMapping("/{userId}")
//    public void generate(@PathVariable String userId,
//                         @RequestParam MultipartFile file,
//                         @RequestParam String settings) throws JsonProcessingException {
//        createDirectory(userId);
//        String scanReportFileName = format("%s/%s", userId, file.getOriginalFilename());
//        storageService.store(file, scanReportFileName);
//
//        ObjectMapper mapper = new ObjectMapper();
//        FakeDataParamsDto dto = mapper.readValue(settings, FakeDataParamsDto.class);
//        dto.setScanReportFileName(scanReportFileName);
//        dto.setDirectory(userId);
//
//        boolean created = this.fakeTasksHandler.createTask(dto, userId);
//        if (!created) {
//            throw new ResponseStatusException(HttpStatus.CONFLICT, "Fake Data Generation Process already run");
//        }
//    }

    @GetMapping("/{userId}")
    public void abort(@PathVariable String userId) {
        fakeTasksHandler.cancelTask(userId);
    }
}
