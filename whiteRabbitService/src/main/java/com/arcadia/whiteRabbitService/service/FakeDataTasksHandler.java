package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.ohdsi.utilities.ConsoleLogger;
import org.ohdsi.utilities.Logger;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

@Service
@RequiredArgsConstructor
public class FakeDataTasksHandler extends AbstractTaskHandler<FakeDataParamsDto, Void> {

    private final WhiteRabbitFacade whiteRabbitFacade;

    @Override
    protected Future<Void> task(FakeDataParamsDto dto, String id) throws FailedToGenerateFakeData {
        Logger logger = new ConsoleLogger();

        return whiteRabbitFacade.generateFakeData(dto, logger);
    }
}
