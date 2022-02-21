package com.arcadia.whiteRabbitService.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.web.multipart.MultipartFile;

@Getter
@RequiredArgsConstructor
public class FileSaveRequest {
    private final String username;
    private final String dataKey;
    private final MultipartFile file;
}
