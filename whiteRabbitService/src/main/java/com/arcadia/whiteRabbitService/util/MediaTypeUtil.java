package com.arcadia.whiteRabbitService.util;

import com.arcadia.whiteRabbitService.service.error.DelimitedTextFileNotSupportedException;
import lombok.SneakyThrows;
import org.ohdsi.whiteRabbit.DbSettings;

import static java.lang.String.format;

public class MediaTypeUtil {

    public static String getBase64HeaderForDelimitedTextFile(DbSettings.SourceType sourceType) {
        return format("data:%s;base64,", getMediaTypeBySourceType(sourceType));
    }

    @SneakyThrows
    private static String getMediaTypeBySourceType(DbSettings.SourceType sourceType) {
        return switch (sourceType) {
            case CSV_FILES -> "text/csv";
            case SAS_FILES -> "application/octet-stream";
            default -> throw new DelimitedTextFileNotSupportedException();
        };
    }
}
