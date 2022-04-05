package com.arcadia.whiteRabbitService.service;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

public interface StorageService {
    String store(MultipartFile file, String directory, String fileName) throws IOException;

    String store(ByteArrayResource resource, String directory, String fileName) throws IOException;
}
