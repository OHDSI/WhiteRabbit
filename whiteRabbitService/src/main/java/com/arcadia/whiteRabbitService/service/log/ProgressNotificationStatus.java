package com.arcadia.whiteRabbitService.service.log;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum ProgressNotificationStatus {
    STARTED(0, "Process started"),
    TABLE_SCANNING(1, "Table scanning"),
    FINISHED(2, "Process finished"),
    ERROR(3, "Error"),
    FAILED(4, "Process failed"),
    CANCELED(5, "Process canceled"),
    NONE(6, "None");

    private final int code;
    private final String description;
}
