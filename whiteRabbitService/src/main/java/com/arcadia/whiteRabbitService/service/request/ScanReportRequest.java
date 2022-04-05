package com.arcadia.whiteRabbitService.service.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScanReportRequest {
    @NotNull
    private Long dataId;
    @NotNull
    private String fileName;
}
