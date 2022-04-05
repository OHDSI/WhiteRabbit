package com.arcadia.whiteRabbitService.service.error;

import org.springframework.web.server.ResponseStatusException;

import static org.springframework.http.HttpStatus.NOT_FOUND;

public class NotFoundException extends ResponseStatusException {
    public NotFoundException(String message) {
        super(NOT_FOUND, message);
    }
}
