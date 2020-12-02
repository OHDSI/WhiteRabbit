package com.arcadia.whiteRabbitService.util;

import org.junit.jupiter.api.Test;

import static com.arcadia.whiteRabbitService.util.Base64Util.removeBase64Header;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class Base64UtilTest {

    @Test
    void removeBase64HeaderTest() {
        var base64Header = "data:application/vnd.ms-excel;base64,";
        var base64Content = "77u/SUQsQklSV";
        var base64 = base64Header + base64Content;

        assertEquals(base64Content, removeBase64Header(base64));
    }
}
