package com.arcadia.whiteRabbitService.service.log;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum ProgressNotificationStatus {
    TABLE_SCANNING(0, "Table scanning"),
    ERROR(1, "Error"),
    SCAN_REPORT_GENERATED(2, "Scan report generated"),
    FAILED_TO_SCAN(3, "Failed to scan"),
    NONE(4, "None");

    private final int code;
    private final String description;
}
