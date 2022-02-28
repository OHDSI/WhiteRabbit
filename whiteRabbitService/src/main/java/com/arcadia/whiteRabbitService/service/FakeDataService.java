package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import org.ohdsi.utilities.Logger;

public interface FakeDataService {
    void generateFakeData(FakeDataParamsDto dto, Logger logger) throws FailedToGenerateFakeData;
}
