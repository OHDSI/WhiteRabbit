package com.arcadia.whiteRabbitService.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class TablesInfoDto {
    private final List<String> tableNames;
}
