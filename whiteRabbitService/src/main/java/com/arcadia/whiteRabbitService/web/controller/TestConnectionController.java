package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import com.arcadia.whiteRabbitService.dto.TestConnectionDto;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/test-connection")
@Validated
@RequiredArgsConstructor
public class TestConnectionController {

    private final WhiteRabbitFacade whiteRabbitFacade;

    @PostMapping
    public TestConnectionDto testConnection(@RequestBody DbSettingsDto dto) {
        return whiteRabbitFacade.testConnection(dto);
    }
}
