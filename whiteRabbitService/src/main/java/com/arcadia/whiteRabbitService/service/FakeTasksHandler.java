package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import com.arcadia.whiteRabbitService.service.log.WebSocketLogger;
import lombok.AllArgsConstructor;
import org.ohdsi.utilities.Logger;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

@Service
@AllArgsConstructor
public class FakeTasksHandler extends AbstractTaskHandler<FakeDataParamsDto, Void> {

    private final WhiteRabbitFacade whiteRabbitFacade;

    private final SimpMessagingTemplate messagingTemplate;

    @Override
    protected Future<Void> task(FakeDataParamsDto dto, String id) throws FailedToGenerateFakeData {
        Logger logger = new WebSocketLogger(messagingTemplate, id, "/queue/reply");

        return whiteRabbitFacade.generateFakeData(dto, logger);
    }
}
