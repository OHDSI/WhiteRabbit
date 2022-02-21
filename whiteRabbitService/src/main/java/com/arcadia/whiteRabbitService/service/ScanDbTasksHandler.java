package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.SettingsDto;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import lombok.RequiredArgsConstructor;
import org.ohdsi.utilities.ConsoleLogger;
import org.ohdsi.utilities.Logger;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

@Service
@RequiredArgsConstructor
public class ScanDbTasksHandler extends AbstractTaskHandler<SettingsDto, String> {

    private final WhiteRabbitFacade whiteRabbitFacade;

    @Override
    protected Future<String> task(SettingsDto dto, String id) throws FailedToScanException {
        Logger logger = new ConsoleLogger();

        return whiteRabbitFacade.generateScanReport(dto, logger);
    }
}
