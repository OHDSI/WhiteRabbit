package com.arcadia.whiteRabbitService.service.log;

import com.arcadia.whiteRabbitService.dto.ProgressNotificationDto;
import lombok.AllArgsConstructor;
import org.ohdsi.utilities.ConsoleLogger;
import org.ohdsi.utilities.Logger;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.util.Map;
import java.util.function.Function;

import static com.arcadia.whiteRabbitService.service.log.ProgressNotificationStatus.*;
import static java.lang.String.format;
import static org.ohdsi.utilities.StringUtilities.now;
import static org.springframework.messaging.simp.SimpMessageHeaderAccessor.create;

@AllArgsConstructor
public class WebSocketLogger implements Logger {

    private final SimpMessagingTemplate messagingTemplate;

    private final String user;

    private final String destination;

    private final Logger consoleLogger = new ConsoleLogger();

    private static final Map<ProgressNotificationStatus, Function<String, Boolean>> messageStatusRecognizers = Map.of(
            TABLE_SCANNING, m -> m.contains("Scanning table"),
            SCAN_REPORT_GENERATED, m -> m.contains("Scan report generated")
    );

    @Override
    public void log(String message) {
        consoleLogger.log(message);
        sendMessageToUser(message, getStatusByMessage(message));
    }

    @Override
    public void logWithTime(String message) {
        consoleLogger.logWithTime(message);
        sendMessageToUser(format("%s\t%s", now(), message), getStatusByMessage(message));
    }

    @Override
    public void error(String message) {
        consoleLogger.error(message);
        sendMessageToUser(message, ERROR);
    }

    private void sendMessageToUser(String message, ProgressNotificationStatus status) {
        messagingTemplate.convertAndSendToUser(
                user, destination,
                new ProgressNotificationDto(message, status),
                createHeaders(user)
        );
    }

    private MessageHeaders createHeaders(String sessionId) {
        SimpMessageHeaderAccessor headerAccessor = create(SimpMessageType.MESSAGE);
        headerAccessor.setSessionId(sessionId);
        headerAccessor.setLeaveMutable(true);
        return headerAccessor.getMessageHeaders();
    }

    private ProgressNotificationStatus getStatusByMessage(String message) {
        for (Map.Entry<ProgressNotificationStatus, Function<String, Boolean>> entry : messageStatusRecognizers.entrySet()) {
            if (entry.getValue().apply(message)) {
                return entry.getKey();
            }
        }

        return NONE;
    }
}
