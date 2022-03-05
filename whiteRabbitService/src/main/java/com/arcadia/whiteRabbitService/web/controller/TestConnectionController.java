package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.model.scandata.ScanDbSettings;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import com.arcadia.whiteRabbitService.service.response.TestConnectionResultResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.ResponseEntity.ok;

@RestController
@RequestMapping("/api/test-connection")
@RequiredArgsConstructor
@Slf4j
public class TestConnectionController {
    private final WhiteRabbitFacade whiteRabbitFacade;

    @PostMapping
    public ResponseEntity<TestConnectionResultResponse> testConnection(@Validated @RequestBody ScanDbSettings dbSetting) {
        log.info("Rest request to test connection with settings {}", dbSetting);
        return ok(whiteRabbitFacade.testConnection(dbSetting));
    }
}
