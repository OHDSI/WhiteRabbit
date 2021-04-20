package com.arcadia.whiteRabbitService.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.nio.file.Path;
import java.util.List;

import static com.arcadia.whiteRabbitService.util.FileUtil.deleteRecursive;

@AllArgsConstructor
@Getter
public class FileSettingsDto implements SettingsDto {
    private final String fileType;

    private final String delimiter;

    private final ScanParamsDto scanParams;

    @Setter
    private String fileDirectory;

    @Setter
    private List<String> fileNames;

    @Override
    public void destroy() {
        deleteRecursive(Path.of(fileDirectory));
    }
}
