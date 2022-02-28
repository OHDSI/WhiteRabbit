package com.arcadia.whiteRabbitService.service;

import lombok.SneakyThrows;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.String.format;

/**
** Store csv file for scanning via WhiteRabbit
**/
@Service
public class StorageServiceImpl implements StorageService {
    @SneakyThrows
    public String store(MultipartFile multipartFile, String directory, String fileName) {
        String path = format("%s/%s", directory, fileName);
        try (OutputStream os = Files.newOutputStream(Path.of(path))) {
            os.write(multipartFile.getBytes());
        }
        return path;
    }
}
