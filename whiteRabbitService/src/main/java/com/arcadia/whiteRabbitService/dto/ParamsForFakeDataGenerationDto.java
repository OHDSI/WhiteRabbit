package com.arcadia.whiteRabbitService.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ParamsForFakeDataGenerationDto {

    private final int maxRowCount;

    private final boolean doUniformSampling;

    private final String scanReportBase64;

    private final DbSettingsDto dbSettings;
}
