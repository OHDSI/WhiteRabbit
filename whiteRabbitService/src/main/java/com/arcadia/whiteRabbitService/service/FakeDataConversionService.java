package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;

import java.util.concurrent.Future;

public interface FakeDataConversionService {
    Future<Void> runConversion(FakeDataConversion conversion);
}
