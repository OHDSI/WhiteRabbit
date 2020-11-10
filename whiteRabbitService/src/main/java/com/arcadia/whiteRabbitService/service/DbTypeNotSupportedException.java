package com.arcadia.whiteRabbitService.service;

public class DbTypeNotSupportedException extends Exception {
    DbTypeNotSupportedException() {
        super("Database type not supported");
    }
}
