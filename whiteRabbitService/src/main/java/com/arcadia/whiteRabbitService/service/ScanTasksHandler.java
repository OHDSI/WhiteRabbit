package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import com.arcadia.whiteRabbitService.dto.SettingsDto;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import com.arcadia.whiteRabbitService.service.log.WebSocketLogger;
import lombok.AllArgsConstructor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

@Service
@AllArgsConstructor
public class ScanTasksHandler extends AbstractTaskHandler<SettingsDto, String> {

    private final WhiteRabbitFacade whiteRabbitFacade;

    private final SimpMessagingTemplate messagingTemplate;

    @Override
    protected Future<String> task(SettingsDto dto, String id) throws FailedToScanException {
        WebSocketLogger logger = new WebSocketLogger(messagingTemplate, id, "/queue/reply");

        return whiteRabbitFacade.generateScanReport(dto, logger);
    }
}
