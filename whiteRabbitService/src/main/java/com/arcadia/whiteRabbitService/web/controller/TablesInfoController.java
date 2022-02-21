package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import com.arcadia.whiteRabbitService.dto.TablesInfoDto;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/tables-info")
@RequiredArgsConstructor
public class TablesInfoController {

    private final WhiteRabbitFacade whiteRabbitFacade;

    @PostMapping
    public TablesInfoDto tablesInfo(@RequestBody DbSettingsDto dto) {
        try {
            return whiteRabbitFacade.tablesInfo(dto);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e.getCause());
        }
    }
}
