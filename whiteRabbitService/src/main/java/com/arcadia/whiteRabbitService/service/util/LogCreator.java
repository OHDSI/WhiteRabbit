package com.arcadia.whiteRabbitService.service.util;

import com.arcadia.whiteRabbitService.model.LogStatus;

public interface LogCreator<T> {
    T create(String message, LogStatus status, Integer percent);
}
