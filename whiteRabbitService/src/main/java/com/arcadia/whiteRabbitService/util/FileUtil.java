package com.arcadia.whiteRabbitService.util;

import lombok.SneakyThrows;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Base64.getDecoder;
import static org.apache.commons.lang.RandomStringUtils.random;

public class FileUtil {

    private static final int generatedNameLength = 15;

    @SneakyThrows
    public static Path base64ToFile(Path path, String base64) {
        byte[] decodedFileContent = getDecoder()
                .decode(base64.getBytes(StandardCharsets.UTF_8));
        return Files.write(path, decodedFileContent);
    }

    public static String generateRandomFileName() {
        return random(generatedNameLength, true, false);
    }

    public static String generateRandomDirectory() {
        String directoryName = random(generatedNameLength, true, false);
        File directory = new File(directoryName);
        directory.mkdirs();

        return directoryName;
    }

    @SneakyThrows
    public static void deleteRecursive(Path path) {
        FileSystemUtils.deleteRecursively(path);
    }
}
