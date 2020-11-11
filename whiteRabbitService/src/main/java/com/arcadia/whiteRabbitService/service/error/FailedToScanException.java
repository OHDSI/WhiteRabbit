package com.arcadia.whiteRabbitService.service.error;


public class FailedToScanException extends Exception {
    private static final String message = "Failed to scan";

    public FailedToScanException() {
        super(message);
    }

    public FailedToScanException(Throwable cause) {
        super(message, cause);
    }
}
