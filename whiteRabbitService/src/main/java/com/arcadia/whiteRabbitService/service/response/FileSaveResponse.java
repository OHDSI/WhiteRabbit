package com.arcadia.whiteRabbitService.service.response;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileSaveResponse {
    private Long id;
    private String username;
    private String dataKey;
    private String fileName;
}
