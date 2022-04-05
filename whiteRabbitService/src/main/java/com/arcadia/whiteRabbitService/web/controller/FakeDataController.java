package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataSettings;
import com.arcadia.whiteRabbitService.service.FakeDataService;
import com.arcadia.whiteRabbitService.service.error.BadRequestException;
import com.arcadia.whiteRabbitService.service.request.FakeDataRequest;
import com.arcadia.whiteRabbitService.service.response.ConversionWithLogsResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Validator;

import static javax.validation.Validation.buildDefaultValidatorFactory;
import static org.springframework.http.ResponseEntity.noContent;
import static org.springframework.http.ResponseEntity.ok;

@RestController
@RequestMapping("/api/fake-data")
@RequiredArgsConstructor
@Slf4j
public class FakeDataController {
    public static final String INCORRECT_PARAMS_MESSAGE = "Incorrect Fake Data Params";
    private final FakeDataService fakeDataService;

    @Deprecated
    @PostMapping()
    public ResponseEntity<FakeDataConversion> generate(@RequestHeader("Username") String username,
                                                       @RequestParam MultipartFile file,
                                                       @RequestParam String settings) {
        log.info("Rest request to generate Fake Data. Deprecated use /api/fake-data/generate");
        try {
            ObjectMapper mapper = new ObjectMapper();
            FakeDataSettings fakeDataSettings = mapper.readValue(settings, FakeDataSettings.class);
            Validator validator = buildDefaultValidatorFactory().getValidator();
            if (validator.validate(fakeDataSettings).isEmpty()) {
                return ok(fakeDataService.generateFakeData(file, fakeDataSettings, username));
            } else {
                log.error(INCORRECT_PARAMS_MESSAGE);
                throw new BadRequestException(INCORRECT_PARAMS_MESSAGE);
            }
        } catch (JsonProcessingException e) {
            log.error(INCORRECT_PARAMS_MESSAGE + ". " + e.getMessage());
            throw new BadRequestException(INCORRECT_PARAMS_MESSAGE);
        }
    }

    @PostMapping("/generate")
    public ResponseEntity<FakeDataConversion> generate(@RequestHeader("Username") String username,
                                                       @Validated @RequestBody FakeDataRequest fakeDataRequest) {
        log.info("Rest request to generate Fake Data");
        return ok(fakeDataService.generateFakeData(fakeDataRequest, username));
    }

    @GetMapping("/abort/{conversionId}")
    public ResponseEntity<Void> abort(@RequestHeader("Username") String username,
                                      @PathVariable Long conversionId) {
        log.info("Rest request to abort Fake Data conversion with id {}", conversionId);
        fakeDataService.abort(conversionId, username);
        return noContent().build();
    }

    @GetMapping("/conversion/{conversionId}")
    public ResponseEntity<ConversionWithLogsResponse> conversionInfoAndLogs(@RequestHeader("Username") String username,
                                                                            @PathVariable Long conversionId) {
        log.info("Rest request to get Fake Data Conversion info and logs by Conversion id {}", conversionId);
        return ok(fakeDataService.conversionInfoWithLogs(conversionId, username));
    }
}
