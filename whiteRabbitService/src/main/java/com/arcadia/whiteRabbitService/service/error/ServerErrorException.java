package com.arcadia.whiteRabbitService.service.error;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

public class ServerErrorException extends ResponseStatusException {
    public ServerErrorException(String reason) {
        super(INTERNAL_SERVER_ERROR, reason);
    }

    public ServerErrorException(String reason, Throwable cause) {
        super(INTERNAL_SERVER_ERROR, reason, cause);
    }
}
