package com.arcadia.whiteRabbitService.util;

public class Base64Util {

    public static String removeBase64Header(String base64) {
        var headerEnd = ";base64,";
        var beginIndex = base64.indexOf(headerEnd);
        return base64.substring(beginIndex + headerEnd.length());
    }
}
