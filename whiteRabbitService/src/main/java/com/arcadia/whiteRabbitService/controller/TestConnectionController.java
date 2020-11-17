package com.arcadia.whiteRabbitService.controller;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import com.arcadia.whiteRabbitService.dto.TestConnectionDto;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@AllArgsConstructor
@RestController
@RequestMapping("/dev/white-rabbit-service/api/test-connection")
public class TestConnectionController {

    private final WhiteRabbitFacade whiteRabbitFacade;

    @PostMapping
    public TestConnectionDto testConnection(@RequestBody DbSettingsDto dto) {
        return whiteRabbitFacade.testConnection(dto);
    }
}
