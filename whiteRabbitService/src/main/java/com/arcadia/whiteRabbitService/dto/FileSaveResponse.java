package com.arcadia.whiteRabbitService.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class FileSaveResponse {
    private final String hash;
    private final String username;
    private final String dataKey;
}
