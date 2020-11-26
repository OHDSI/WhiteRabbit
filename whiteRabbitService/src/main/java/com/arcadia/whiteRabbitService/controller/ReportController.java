package com.arcadia.whiteRabbitService.controller;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import com.arcadia.whiteRabbitService.dto.DelimitedTextFileSettingsDto;
import com.arcadia.whiteRabbitService.dto.ProgressNotificationDto;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import com.arcadia.whiteRabbitService.service.log.WebSocketLogger;
import lombok.AllArgsConstructor;
import org.apache.commons.codec.binary.Base64;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.annotation.SendToUser;
import org.springframework.stereotype.Controller;

import static com.arcadia.whiteRabbitService.service.log.ProgressNotificationStatus.FAILED_TO_SCAN;

@AllArgsConstructor
@Controller
public class ReportController {

    private final WhiteRabbitFacade whiteRabbitFacade;

    private final SimpMessagingTemplate messagingTemplate;

    private final String replyDestination = "/queue/reply";

    @MessageMapping("/scan-report/db")
    @SendToUser("/queue/scan-report")
    public String scanReport(@Payload DbSettingsDto dto, @Header("simpSessionId") String sessionId) throws FailedToScanException {
        var logger = new WebSocketLogger(messagingTemplate, sessionId, replyDestination);
        var reportBytes = whiteRabbitFacade.generateScanReport(dto, logger);

        return Base64.encodeBase64String(reportBytes);
    }

    @MessageMapping("/scan-report/file")
    @SendToUser("/queue/scan-report")
    public String scanReport(@Payload DelimitedTextFileSettingsDto dto, @Header("simpSessionId") String sessionId) throws FailedToScanException {
        var logger = new WebSocketLogger(messagingTemplate, sessionId, replyDestination);
        var reportBytes = whiteRabbitFacade.generateScanReport(dto, logger);

        return Base64.encodeBase64String(reportBytes);
    }

    @MessageExceptionHandler
    @SendToUser("/queue/reply")
    public ProgressNotificationDto handleException(Exception exception) {
        return new ProgressNotificationDto(exception.getMessage(), FAILED_TO_SCAN);
    }
}
