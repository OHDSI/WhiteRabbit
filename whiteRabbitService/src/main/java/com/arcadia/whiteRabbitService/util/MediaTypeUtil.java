package com.arcadia.whiteRabbitService.util;

import com.arcadia.whiteRabbitService.service.error.DelimitedTextFileNotSupportedException;
import lombok.SneakyThrows;
import org.ohdsi.whiteRabbit.DbSettings;

public class MediaTypeUtil {

    @SneakyThrows
    public static String getMediaTypeBySourceType(DbSettings.SourceType sourceType) {
        return switch (sourceType) {
            case CSV_FILES -> "text/csv";
            case SAS_FILES -> "application/octet-stream";
            default -> throw new DelimitedTextFileNotSupportedException();
        };
    }
}
