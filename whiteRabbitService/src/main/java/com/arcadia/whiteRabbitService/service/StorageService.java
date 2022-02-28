package com.arcadia.whiteRabbitService.service;

import org.springframework.web.multipart.MultipartFile;

public interface StorageService {
    String store(MultipartFile file, String directory, String fileName);
}
