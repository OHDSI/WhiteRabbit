package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.service.response.TablesInfoResponse;
import com.arcadia.whiteRabbitService.model.scandata.ScanDbSettings;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
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
@RequestMapping("/api/tables-info")
@RequiredArgsConstructor
@Slf4j
public class TablesInfoController {

    private final WhiteRabbitFacade whiteRabbitFacade;

    @PostMapping
    public ResponseEntity<TablesInfoResponse> tablesInfo(@Validated @RequestBody ScanDbSettings dbSetting) {
        log.info("Rest request to extract tables info with settings {}", dbSetting);
        return ok(whiteRabbitFacade.tablesInfo(dbSetting));
    }
}
