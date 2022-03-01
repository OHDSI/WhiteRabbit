package com.arcadia.whiteRabbitService.service.response;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileSaveResponse {
    private String hash;
    private String username;
    private String dataKey;
}
