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

/*
* Store csv file for scanning via WhiteRabbit
* */
@Service
public class FileStorageService implements StorageService {

    @SneakyThrows
    public void store(MultipartFile multipartFile, String name) {
        try (OutputStream os = Files.newOutputStream(Path.of(name))) {
            os.write(multipartFile.getBytes());
        }
    }

    @SneakyThrows(MalformedURLException.class)
    public Resource loadAsResource(String name) throws FileNotFoundException {
        File file = new File(name);
        UrlResource resource = new UrlResource(file.toURI());
        if (resource.exists() && resource.isReadable()) {
            return resource;
        } else {
            throw new FileNotFoundException("File not found");
        }
    }

    public boolean delete(String name) {
        File file = new File(name);
        return file.delete();
    }
}
