package com.arcadia.whiteRabbitService.service.error;

public class DbTypeNotSupportedException extends BadRequestException {
    public DbTypeNotSupportedException() {
        super("Database type not supported");
    }
}
