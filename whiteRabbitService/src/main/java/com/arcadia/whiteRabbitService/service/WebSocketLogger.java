package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.ProgressNotificationDto;
import lombok.AllArgsConstructor;
import org.ohdsi.utilities.ConsoleLogger;
import org.ohdsi.utilities.Logger;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import static org.ohdsi.utilities.StringUtilities.now;

@AllArgsConstructor
public class WebSocketLogger implements Logger {

    private final SimpMessagingTemplate messagingTemplate;

    private final String user;

    private final String destination;

    private final Logger consoleLogger = new ConsoleLogger();

    @Override
    public void log(String message) {
        consoleLogger.log(message);
        sendMessageToUser(message);
    }

    @Override
    public void logWithTime(String message) {
        consoleLogger.logWithTime(message);
        sendMessageToUser(now() + "\t" + message);
    }

    @Override
    public void error(String message) {
        consoleLogger.log(message);
        sendMessageToUser(message);
    }

    private void sendMessageToUser(String message) {
        messagingTemplate.convertAndSendToUser(
                user, destination,
                new ProgressNotificationDto(message),
                createHeaders(user)
        );
    }

    private MessageHeaders createHeaders(String sessionId) {
        SimpMessageHeaderAccessor headerAccessor = SimpMessageHeaderAccessor.create(SimpMessageType.MESSAGE);
        headerAccessor.setSessionId(sessionId);
        headerAccessor.setLeaveMutable(true);
        return headerAccessor.getMessageHeaders();
    }
}
