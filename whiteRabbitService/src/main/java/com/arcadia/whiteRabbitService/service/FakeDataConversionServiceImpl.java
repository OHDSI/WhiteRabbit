package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataLog;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataSettings;
import com.arcadia.whiteRabbitService.repository.FakeDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.FakeDataLogRepository;
import com.arcadia.whiteRabbitService.service.util.DatabaseLogger;
import com.arcadia.whiteRabbitService.service.util.FakeDataLogCreator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

@Service
@RequiredArgsConstructor
@Slf4j
public class FakeDataConversionServiceImpl implements FakeDataConversionService {
    private final FakeDataLogRepository logRepository;
    private final FakeDataConversionRepository conversionRepository;
    private final WhiteRabbitFacade whiteRabbitFacade;
    private final FakeDataResultService resultService;

    @Async
    @Override
    public Future<Void> runConversion(FakeDataConversion conversion) {
        FakeDataSettings settings = conversion.getFakeDataSettings();
        FakeDataLogCreator logCreator = new FakeDataLogCreator(conversion);
        DatabaseLogger<FakeDataLog> logger = new DatabaseLogger<>(logRepository, logCreator);
        FakeDataInterrupter interrupter = new FakeDataInterrupter(conversionRepository, conversion.getId());
        try {
            whiteRabbitFacade.generateFakeData(settings, logger, interrupter);
            resultService.saveCompletedResult(conversion.getId());
        } catch (InterruptedException e) {
            log.warn(e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
            resultService.saveFailedResult(conversion.getId(), e.getMessage());
        } finally {
            settings.destroy();
        }
        return new AsyncResult<>(null);
    }
}
