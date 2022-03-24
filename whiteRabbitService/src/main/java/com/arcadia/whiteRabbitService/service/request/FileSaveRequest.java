package com.arcadia.whiteRabbitService.service.request;

import lombok.*;
import org.springframework.core.io.FileSystemResource;
import org.springframework.web.multipart.MultipartFile;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileSaveRequest {
    private String username;
    private String dataKey;
    private FileSystemResource file;
}
