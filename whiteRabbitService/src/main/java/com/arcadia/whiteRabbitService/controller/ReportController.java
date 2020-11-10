package com.arcadia.whiteRabbitService.controller;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import com.arcadia.whiteRabbitService.dto.ScanReportDto;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.codec.binary.Base64;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;

@AllArgsConstructor
@Controller
public class ReportController {

    private final WhiteRabbitFacade whiteRabbitFacade;

    @SneakyThrows
    @MessageMapping("/scan-report")
    @SendTo("/reports/scan-report")
    public ScanReportDto scanReport(DbSettingsDto dto) {
        var reportBytes = whiteRabbitFacade.generateScanReport(dto);

        return new ScanReportDto(Base64.encodeBase64String(reportBytes));
    }
}
