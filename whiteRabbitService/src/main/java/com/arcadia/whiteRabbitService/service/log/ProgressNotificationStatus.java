package com.arcadia.whiteRabbitService.service.log;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum ProgressNotificationStatus {
    STARTED_SCANNING(0, "Started scanning tables"),
    TABLE_SCANNING(1, "Table scanning"),
    SCAN_REPORT_GENERATED(2, "Scan report generated"),
    ERROR(3, "Error"),
    FAILED_TO_SCAN(4, "Failed to scan"),
    CANCELED(5, "Scan process canceled"),
    NONE(6, "None");

    private final int code;
    private final String description;
}
