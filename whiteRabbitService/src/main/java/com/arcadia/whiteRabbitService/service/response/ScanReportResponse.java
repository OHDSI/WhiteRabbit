package com.arcadia.whiteRabbitService.service.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScanReportResponse {
    private Long dataId;
    private String fileName;
}
