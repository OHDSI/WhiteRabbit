package com.arcadia.whiteRabbitService.service;

import lombok.SneakyThrows;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Optional;


class FilesManagerServiceTest {

    @SneakyThrows
    static byte[] readFileFromResourcesAsByteArray(Class<?> currentClass, String fileName) {
        InputStream inputStream = Optional.ofNullable(currentClass.getClassLoader().getResourceAsStream(fileName))
                .orElseThrow(() -> new RuntimeException("Can not open file " + fileName));
        return inputStream.readAllBytes();
    }

    @SneakyThrows
    static File readFileFromResources(Class<?> currentClass, String fileName) {
        URL resource = Optional.ofNullable(currentClass.getClassLoader().getResource(fileName))
                .orElseThrow(() -> new RuntimeException("Can not open file " + fileName));
        return new File(resource.toURI());
    }
}