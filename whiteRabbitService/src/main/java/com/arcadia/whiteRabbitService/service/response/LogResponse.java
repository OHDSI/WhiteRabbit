package com.arcadia.whiteRabbitService.service.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogResponse {
    private String message;
    private Integer statusCode;
    private String statusName;
    private Integer percent;
}
