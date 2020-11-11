package com.arcadia.whiteRabbitService.service.error;

public class DbTypeNotSupportedException extends Exception {
    public DbTypeNotSupportedException() {
        super("Database type not supported");
    }
}
