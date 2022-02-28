package com.arcadia.whiteRabbitService.service.response;

import lombok.*;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TablesInfoResponse {
    private List<String> tableNames;
}
