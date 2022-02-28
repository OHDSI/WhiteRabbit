package com.arcadia.whiteRabbitService.service.response;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TestConnectionResultResponse {
    private boolean canConnect;

    private String message;
}
