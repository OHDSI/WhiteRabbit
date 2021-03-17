package com.arcadia.whiteRabbitService.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class FakeDataParamsDto {

    private final Integer maxRowCount;

    private final Boolean doUniformSampling;

    private final String schema;

    private final String scanReportBase64;
}
