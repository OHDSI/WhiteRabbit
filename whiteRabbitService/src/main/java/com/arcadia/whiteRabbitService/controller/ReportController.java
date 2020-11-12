package com.arcadia.whiteRabbitService.controller;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import com.arcadia.whiteRabbitService.dto.DelimitedTextFileSettingsDto;
import com.arcadia.whiteRabbitService.dto.ProgressNotificationDto;
import com.arcadia.whiteRabbitService.dto.ScanReportDto;
import com.arcadia.whiteRabbitService.service.WebSocketLogger;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import lombok.AllArgsConstructor;
import org.apache.commons.codec.binary.Base64;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.annotation.SendToUser;
import org.springframework.stereotype.Controller;

@AllArgsConstructor
@Controller
public class ReportController {

    private final WhiteRabbitFacade whiteRabbitFacade;

    private final SimpMessagingTemplate messagingTemplate;

    @MessageMapping("/scan-report/db")
    @SendToUser("/queue/scan-report")
    public ScanReportDto scanReport(@Payload DbSettingsDto dto, @Header("simpSessionId") String sessionId) throws FailedToScanException {
        var logger = new WebSocketLogger(messagingTemplate, sessionId, "/queue/reply");
        var reportBytes = whiteRabbitFacade.generateScanReport(dto, logger);

        return new ScanReportDto(Base64.encodeBase64String(reportBytes));
    }

    @MessageMapping("/scan-report/file")
    @SendToUser("/queue/scan-report")
    public ScanReportDto scanReport(@Payload DelimitedTextFileSettingsDto dto, @Header("simpSessionId") String sessionId) throws FailedToScanException {
        var logger = new WebSocketLogger(messagingTemplate, sessionId, "/queue/reply");
        var reportBytes = whiteRabbitFacade.generateScanReport(dto, logger);

        return new ScanReportDto(Base64.encodeBase64String(reportBytes));
    }

    @MessageExceptionHandler
    @SendToUser("/queue/reply")
    public ProgressNotificationDto handleException(Exception exception) {
        return new ProgressNotificationDto(exception.getMessage());
    }
}
