package com.arcadia.whiteRabbitService.dto;

import com.arcadia.whiteRabbitService.service.log.ProgressNotificationStatus;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ProgressNotificationDto {
    private final String message;

    private final ProgressNotificationStatus status;
}
