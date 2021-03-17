package com.arcadia.whiteRabbitService.service.error;

public class DelimitedTextFileNotSupportedException extends Exception {
    public DelimitedTextFileNotSupportedException() {
        super("Delimited text file not supported");
    }
}
