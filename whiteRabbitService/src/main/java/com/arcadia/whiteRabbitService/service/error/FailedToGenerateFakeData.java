package com.arcadia.whiteRabbitService.service.error;

public class FailedToGenerateFakeData extends Exception {
    private final static String message = "Failed to generate fake data";

    public FailedToGenerateFakeData() {
        super(message);
    }

    public FailedToGenerateFakeData(Throwable cause) {
        super(message, cause);
    }
}
