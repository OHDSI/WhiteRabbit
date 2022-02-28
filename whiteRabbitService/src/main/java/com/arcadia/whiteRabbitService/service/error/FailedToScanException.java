package com.arcadia.whiteRabbitService.service.error;


public class FailedToScanException extends RuntimeException {
    public FailedToScanException(String message) {
        super(message);
    }
}
