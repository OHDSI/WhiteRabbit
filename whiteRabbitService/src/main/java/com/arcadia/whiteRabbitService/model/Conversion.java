package com.arcadia.whiteRabbitService.model;

import java.util.List;

public interface Conversion<T extends Log> {
    Long getId();
    Integer getStatusCode();
    String getStatusName();
    List<T> getLogs();
    void setStatus(ConversionStatus status);
}
