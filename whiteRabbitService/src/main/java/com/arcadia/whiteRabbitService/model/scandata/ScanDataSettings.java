package com.arcadia.whiteRabbitService.model.scandata;

import org.ohdsi.whiteRabbit.DbSettings;

public interface ScanDataSettings {
    ScanDataParams getScanDataParams();

    DbSettings toWhiteRabbitSettings();

    String scanReportFileName();

    default void destroy() {}
}
