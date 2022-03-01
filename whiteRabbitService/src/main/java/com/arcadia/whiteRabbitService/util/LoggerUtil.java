package com.arcadia.whiteRabbitService.util;

public class LoggerUtil {
    public static final int LOG_MESSAGE_MAX_LENGTH = 1000;

    public static String checkMessageAndSubstringIfNeeded(String message) {
        return message.length() > LOG_MESSAGE_MAX_LENGTH ?
                message.substring(0, LOG_MESSAGE_MAX_LENGTH) :
                message;
    }
}
