package com.arcadia.whiteRabbitService.service.error;

public class DelimitedTextFileNotSupportedException extends BadRequestException {
    public DelimitedTextFileNotSupportedException() {
        super("Delimited text file not supported");
    }
}
