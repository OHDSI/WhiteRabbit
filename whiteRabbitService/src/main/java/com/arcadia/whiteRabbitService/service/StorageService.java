package com.arcadia.whiteRabbitService.service;

import org.springframework.core.io.Resource;
import org.springframework.web.multipart.MultipartFile;

import java.io.FileNotFoundException;

public interface StorageService {

    void store(MultipartFile file, String name);

    Resource loadAsResource(String name) throws FileNotFoundException;

    boolean delete(String name);
}
