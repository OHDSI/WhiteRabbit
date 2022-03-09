package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.config.FakeDataDbConfig;
import com.arcadia.whiteRabbitService.model.fakedata.FakeDataSettings;
import com.arcadia.whiteRabbitService.service.error.ServerErrorException;
import com.arcadia.whiteRabbitService.service.response.TablesInfoResponse;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataParams;
import com.arcadia.whiteRabbitService.model.scandata.ScanDataSettings;
import com.arcadia.whiteRabbitService.model.scandata.ScanDbSettings;
import com.arcadia.whiteRabbitService.service.response.TestConnectionResultResponse;
import lombok.RequiredArgsConstructor;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.whiteRabbit.Logger;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.Interrupter;
import org.ohdsi.whiteRabbit.fakeDataGenerator.FakeDataGenerator;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.List;

import static com.arcadia.whiteRabbitService.util.DbSettingsAdapter.adaptDbSettings;
import static com.arcadia.whiteRabbitService.util.FileUtil.*;
import static com.arcadia.whiteRabbitService.util.SourceDataScanBuilder.createSourceDataScan;
import static java.lang.String.format;

@Service
@RequiredArgsConstructor
public class WhiteRabbitFacade {
    private final FakeDataDbConfig fakeDataDbConfig;

    @PostConstruct
    public void init() {
        createDirectory(scanReportLocation);
    }

    public TestConnectionResultResponse testConnection(ScanDbSettings dbSetting) {
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

    public TablesInfoResponse tablesInfo(ScanDbSettings dbSetting) {
        DbSettings dbSettings = adaptDbSettings(dbSetting);
        try (RichConnection connection = new RichConnection(
                dbSettings.server,
                dbSettings.domain,
                dbSettings.user,
                dbSettings.password,
                dbSettings.dbType
        )) {
            return new TablesInfoResponse(connection.getTableNames(dbSettings.database));
        } catch (Exception e) {
            throw new ServerErrorException(e.getMessage(), e);
        }
    }

    public File generateScanReport(ScanDataSettings scanDataSettings, Logger logger, Interrupter interrupter) throws InterruptedException {
        DbSettings dbSettings = scanDataSettings.toWhiteRabbitSettings();
        ScanDataParams scanDataParams = scanDataSettings.getScanDataParams();
        SourceDataScan sourceDataScan = createSourceDataScan(scanDataParams, logger, interrupter);
        String scanReportFilePath = toScanReportFileFullName(generateRandomFileName());
        sourceDataScan.process(dbSettings, scanReportFilePath);
        return new File(scanReportFilePath);
    }

    public void generateFakeData(FakeDataSettings fakeDataSettings, Logger logger, Interrupter interrupter) throws InterruptedException {
        DbSettings dbSettings = createFakeDataDbSettings(fakeDataSettings.getUserSchema());
        FakeDataGenerator generator = new FakeDataGenerator();
        generator.setLogger(logger);
        generator.setInterrupter(interrupter);
        String scanReportPath = fakeDataSettings.getDirectory() + "/" + fakeDataSettings.getScanReportFileName();
        generator.generateData(
                dbSettings,
                fakeDataSettings.getMaxRowCount(),
                scanReportPath,
                null, // Not needed, it needs if generate fake data to delimited text file
                fakeDataSettings.getDoUniformSampling(),
                fakeDataSettings.getUserSchema(),
                false // False - Tables are created when the report is uploaded to Perseus python service
        );
    }

    private DbSettings createFakeDataDbSettings(String schema) {
        ScanDbSettings scanDbSettings = ScanDbSettings.builder()
                .dbType(fakeDataDbConfig.getDbType())
                .server(fakeDataDbConfig.getServer())
                .port(fakeDataDbConfig.getPort())
                .database(fakeDataDbConfig.getDatabase())
                .user(fakeDataDbConfig.getUser())
                .password(fakeDataDbConfig.getPassword())
                .schema(schema)
                .build();
        return adaptDbSettings(scanDbSettings);
    }
}
