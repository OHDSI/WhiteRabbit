package com.arcadia.whiteRabbitService.service;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.String.format;

@Service
public class StorageServiceImpl implements StorageService {
    @Override
    public String store(MultipartFile multipartFile, String directory, String fileName) throws IOException {
        String path = format("%s/%s", directory, fileName);
        try (OutputStream os = Files.newOutputStream(Path.of(path))) {
            os.write(multipartFile.getBytes());
        }
        return path;
    }

    @Override
    public String store(ByteArrayResource resource, String directory, String fileName) throws IOException {
        String path = format("%s/%s", directory, fileName);
        try (OutputStream os = Files.newOutputStream(Path.of(path))) {
            os.write(resource.getByteArray());
        }
        return path;
    }
}
