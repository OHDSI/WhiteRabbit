package com.arcadia.whiteRabbitService.service;

public interface FakeDataResultService {
    void saveCompletedResult(Long conversionId);

    void saveFailedResult(Long conversionId, String errorMessage);
}
