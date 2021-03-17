package com.arcadia.whiteRabbitService.util;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class FileUtilTest {

    @Test
    void base64ToFile() {
        String directoryName = FileUtil.generateRandomDirectory();
        String fileName = FileUtil.generateRandomFileName();

        Path path = FileUtil.base64ToFile(Paths.get(directoryName, fileName), base64());

        assertAll("It should be equals to directory and file names",
                () -> assertEquals(directoryName, path.getParent().toString()),
                () -> assertEquals(fileName, path.getFileName().toString())
        );

        FileUtil.deleteRecursive(Paths.get(directoryName));
    }

    @SneakyThrows
    @Test
    void deleteRecursive() {
        String directoryName = FileUtil.generateRandomDirectory();
        String fileName = FileUtil.generateRandomFileName();

        FileUtil.base64ToFile(Paths.get(directoryName, fileName), base64());

        FileUtil.deleteRecursive(Paths.get(directoryName));

        assertAll("It should remove directory recursive",
                () -> assertFalse(new File(directoryName).exists())
        );
    }

    private String base64() {
        final String base64 = "iVBORw0KGgoAAAANSUhEUgA\n" +
                "AAAoAAAAKCAYAAACNMs+9AAAABmJLR0QA/wD/AP+gvaeTAAAAB3RJ\n" +
                "TUUH1ggDCwMADQ4NnwAAAFVJREFUGJWNkMEJADEIBEcbSDkXUnfSg\n" +
                "nBVeZ8LSAjiwjyEQXSFEIcHGP9oAi+H0Bymgx9MhxbFdZE2a0s9kT\n" +
                "Zdw01ZhhYkABSwgmf1Z6r1SNyfFf4BZ+ZUExcNUQUAAAAASUVORK5\n" +
                "CYII=";

        return base64.replace("\n", "");
    }
}
