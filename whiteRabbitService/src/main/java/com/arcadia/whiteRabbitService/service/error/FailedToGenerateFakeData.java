package com.arcadia.whiteRabbitService.service.error;

public class FailedToGenerateFakeData extends RuntimeException {
    public FailedToGenerateFakeData(String message) {
        super(message);
    }

    public FailedToGenerateFakeData(String message, Throwable cause) {
        super(message, cause);
    }
}
