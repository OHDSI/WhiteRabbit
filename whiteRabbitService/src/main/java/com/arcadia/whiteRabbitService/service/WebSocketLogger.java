package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.ProgressNotificationDto;
import lombok.AllArgsConstructor;
import org.ohdsi.utilities.ConsoleLogger;
import org.ohdsi.utilities.Logger;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import static org.ohdsi.utilities.StringUtilities.now;

@AllArgsConstructor
@Service
public class WebSocketLogger implements Logger {

    private final SimpMessagingTemplate messagingTemplate;

    private final String destination = "/reports/scan-report";

    private final Logger consoleLogger = new ConsoleLogger();

    @Override
    public void log(String message) {
        consoleLogger.log(message);
        messagingTemplate.convertAndSend(destination, new ProgressNotificationDto(message));
    }

    @Override
    public void logWithTime(String message) {
        consoleLogger.logWithTime(message);
        messagingTemplate.convertAndSend(destination, new ProgressNotificationDto(now() + "\t" + message));
    }

    @Override
    public void error(String message) {
        consoleLogger.log(message);
        messagingTemplate.convertAndSend(destination, new ProgressNotificationDto(message));
    }
}
