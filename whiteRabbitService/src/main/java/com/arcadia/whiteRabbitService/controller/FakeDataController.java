package com.arcadia.whiteRabbitService.controller;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.dto.ProgressNotificationDto;
import com.arcadia.whiteRabbitService.service.FakeTasksHandler;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import com.arcadia.whiteRabbitService.service.log.WebSocketLogger;
import lombok.AllArgsConstructor;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.annotation.SendToUser;
import org.springframework.stereotype.Controller;

import java.util.concurrent.Future;

import static com.arcadia.whiteRabbitService.service.log.ProgressNotificationStatus.FAILED;

@Controller
@AllArgsConstructor
public class FakeDataController {

    private final WhiteRabbitFacade whiteRabbitFacade;

    private final SimpMessagingTemplate messagingTemplate;

    private final FakeTasksHandler fakeTasksHandler;

    @MessageMapping("/fake-data")
    @SendToUser("/queue/fake-data")
    public String generate(@Payload FakeDataParamsDto dto, @Header("simpSessionId") String sessionId) throws FailedToGenerateFakeData {
        var replyDestination = "/queue/reply";
        var logger = new WebSocketLogger(messagingTemplate, sessionId, replyDestination);

        final Future<Void> future = whiteRabbitFacade.generateFakeData(dto, logger);

        fakeTasksHandler.handleTask(sessionId, future);

        return "Succeeded";
    }

    @MessageExceptionHandler
    @SendToUser("/queue/reply")
    public ProgressNotificationDto handleException(Exception exception) {
        return new ProgressNotificationDto(exception.getMessage(), FAILED);
    }
}
