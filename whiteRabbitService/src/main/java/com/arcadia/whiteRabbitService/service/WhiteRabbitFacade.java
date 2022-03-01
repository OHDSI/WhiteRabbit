package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.service.response.TablesInfoResponse;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataParams;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataSettings;
import com.arcadia.whiteRabbitService.model.scandata.ScanDbSetting;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import com.arcadia.whiteRabbitService.service.response.TestConnectionResultResponse;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.whiteRabbit.Logger;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.Interrupter;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.List;
import java.util.concurrent.Future;

import static com.arcadia.whiteRabbitService.util.DbSettingsAdapter.adaptDbSettings;
import static com.arcadia.whiteRabbitService.util.FileUtil.*;
import static com.arcadia.whiteRabbitService.util.SourceDataScanBuilder.createSourceDataScan;
import static java.lang.String.format;

@Service
public class WhiteRabbitFacade {
    @PostConstruct
    public void init() {
        createDirectory(scanReportLocation);
    }

    public TestConnectionResultResponse testConnection(ScanDbSetting dbSetting) {
        try {
            DbSettings dbSettings = adaptDbSettings(dbSetting);
            RichConnection connection = new RichConnection(
                    dbSettings.server,
                    dbSettings.domain,
                    dbSettings.user,
                    dbSettings.password,
                    dbSettings.dbType
            );
            List<String> tableNames = connection.getTableNames(dbSettings.database);
            if (tableNames.size() == 0) {
                String errorMessage = "Unable to retrieve table names for database " + dbSettings.database;
                return new TestConnectionResultResponse(false, errorMessage);
            }
            connection.close();
            var successMessage = format(
                    "Successfully connected to %s on server %s",
                    dbSettings.database,
                    dbSettings.server
            );

            return new TestConnectionResultResponse(true, successMessage);
        } catch (Exception e) {
            var errorMessage = format("Could not connect to database: %s", e.getMessage());

            return new TestConnectionResultResponse(false, errorMessage);
        }
    }

    public TablesInfoResponse tablesInfo(ScanDbSetting dbSetting) {
        DbSettings dbSettings = adaptDbSettings(dbSetting);
        try (RichConnection connection = new RichConnection(
                dbSettings.server,
                dbSettings.domain,
                dbSettings.user,
                dbSettings.password,
                dbSettings.dbType
        )) {
            return new TablesInfoResponse(connection.getTableNames(dbSettings.database));
        }
    }

    public File generateScanReport(ScanDataSettings scanDataSettings, Logger logger, Interrupter interrupter) throws InterruptedException {
        DbSettings dbSettings = scanDataSettings.toWhiteRabbitSettings();
        int tablesCount = dbSettings.tables.size();
        logger.setItemsCount(tablesCount);
        ScanDataParams scanDataParams = scanDataSettings.getScanDataParams();
        SourceDataScan sourceDataScan = createSourceDataScan(scanDataParams, logger, interrupter);
        String scanReportFilePath = toScanReportFileFullName(generateRandomFileName());
        sourceDataScan.process(dbSettings, scanReportFilePath);
        return new File(scanReportFilePath);
    }

    @Async
    public Future<Void> generateFakeData(FakeDataParamsDto dto, Logger logger) throws FailedToGenerateFakeData {
        return new AsyncResult<>(null);
    }
}
