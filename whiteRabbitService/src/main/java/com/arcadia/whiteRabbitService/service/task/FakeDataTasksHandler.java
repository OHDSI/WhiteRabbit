package com.arcadia.whiteRabbitService.service.task;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import lombok.RequiredArgsConstructor;
import org.ohdsi.whiteRabbit.ConsoleLogger;
import org.ohdsi.whiteRabbit.Logger;
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
