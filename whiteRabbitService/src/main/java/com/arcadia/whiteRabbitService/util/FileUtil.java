package com.arcadia.whiteRabbitService.util;

import lombok.SneakyThrows;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.arcadia.whiteRabbitService.service.Constants.pathToDelimitedTextFiles;
import static java.util.Base64.getDecoder;

public class FileUtil {
    @SneakyThrows
    public static Path base64ToFile(String fileName, String base64) {
        byte[] decodedFileContent = getDecoder()
                .decode(base64.getBytes(StandardCharsets.UTF_8));
        return Files.write(Paths.get(getDirectoryForDelimitedTextFiles(), fileName), decodedFileContent);
    }

    private static String getDirectoryForDelimitedTextFiles() {
        String directoryName = pathToDelimitedTextFiles;
        File directory = new File(directoryName);
        directory.mkdirs();

        return directoryName;
    }
}
