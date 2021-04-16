package com.arcadia.whiteRabbitService.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@AllArgsConstructor
@Getter
public class FileSettingsDto implements SettingsDto {
    private final String fileType;

    private final String delimiter;

    @Setter
    private String fileDirectory;

    @Setter
    private List<String> fileNames;

    private final ScanParamsDto scanParams;
}
