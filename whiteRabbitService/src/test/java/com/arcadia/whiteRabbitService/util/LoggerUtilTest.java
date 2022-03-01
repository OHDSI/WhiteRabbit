package com.arcadia.whiteRabbitService.util;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.api.Test;

import static com.arcadia.whiteRabbitService.util.LoggerUtil.LOG_MESSAGE_MAX_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoggerUtilTest {
    @Test
    void checkMessageAndSubstringIfNeeded() {
        int messageLength = LOG_MESSAGE_MAX_LENGTH + 100;
        String longMessage = generateRandomString(messageLength);

        assertEquals(messageLength, longMessage.length());

        String message = LoggerUtil.checkMessageAndSubstringIfNeeded(longMessage);

        assertEquals(LOG_MESSAGE_MAX_LENGTH, message.length());
        assertEquals(longMessage.substring(0, 1000), message);
    }

    public static String generateRandomString(int length) {
        return RandomStringUtils.random(length, true, true);
    }
}