package com.arcadia.whiteRabbitService.service.request;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.FileSystemResource;
import org.springframework.web.multipart.MultipartFile;

@Getter
@RequiredArgsConstructor
public class FileSaveRequest {
    private final String username;
    private final String dataKey;
    private final FileSystemResource scanReport;
}
