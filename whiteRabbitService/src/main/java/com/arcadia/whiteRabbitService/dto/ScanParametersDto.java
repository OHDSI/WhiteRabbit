package com.arcadia.whiteRabbitService.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ScanParametersDto {
    private final Integer sampleSize;

    private final Boolean scanValues;

    private final Integer minCellCount;

    private final Integer maxValues;

    private final Boolean calculateNumericStats;

    private final Integer numericStatsSamplerSize;
}
