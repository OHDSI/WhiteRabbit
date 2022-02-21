package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.FileSaveRequest;
import com.arcadia.whiteRabbitService.service.response.FileSaveResponse;
import org.apache.catalina.webresources.FileResource;
import org.springframework.core.io.ByteArrayResource;

public interface FilesManagerService {
    ByteArrayResource getFile(String hash);

    FileSaveResponse saveFile(FileSaveRequest request);
}
