package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import com.arcadia.whiteRabbitService.dto.TablesInfoDto;
import com.arcadia.whiteRabbitService.dto.TestConnectionDto;
import com.arcadia.whiteRabbitService.service.error.DbTypeNotSupportedException;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import lombok.AllArgsConstructor;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.Logger;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.springframework.stereotype.Service;

import java.io.File;

import static com.arcadia.whiteRabbitService.service.Constants.scanReportFileName;
import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.adapt;
import static java.lang.String.format;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.readAllBytes;

@AllArgsConstructor
@Service
public class WhiteRabbitFacade {

    public byte[] generateScanReport(DbSettingsDto dto, Logger logger) throws FailedToScanException {
        try {
            DbSettings dbSettings = adapt(dto);

            SourceDataScan sourceDataScan = new SourceDataScanBuilder()
                    .setSampleSize(dto.getSampleSize())
                    .setScanValues(dto.getScanValues())
                    .setMinCellCount(dto.getMinCellCount())
                    .setMaxValues(dto.getMaxValues())
                    .setCalculateNumericStats(dto.getCalculateNumericStats())
                    .setNumericStatsSamplerSize(dto.getNumericStatsSamplerSize())
                    .setLogger(logger)
                    .build();
            sourceDataScan.process(dbSettings, scanReportFileName);

            var reportFile = new File(scanReportFileName);
            var reportPath = reportFile.toPath();
            var reportBytes = readAllBytes(reportPath);

            delete(reportPath);

            return reportBytes;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new FailedToScanException(e.getCause());
        }
    }

    public TestConnectionDto testConnection(DbSettingsDto dto) {
        try {
            DbSettings dbSettings = adapt(dto);
            RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain,
                    dbSettings.user, dbSettings.password, dbSettings.dbType);

            var tableNames = connection.getTableNames(dbSettings.database);
            if (tableNames.size() == 0) {
                throw new Exception("Unable to retrieve table names for database " + dbSettings.database);
            }

            connection.close();

            var successMessage = format("Successfully connected to %s on server %s", dbSettings.database, dbSettings.server);
            return new TestConnectionDto(true, successMessage);

        } catch (Exception e) {
            var errorMessage = format("Could not connect to database: %s", e.getMessage());
            return new TestConnectionDto(false, errorMessage);
        }
    }

    public TablesInfoDto tablesInfo(DbSettingsDto dto) throws DbTypeNotSupportedException {
        DbSettings dbSettings = adapt(dto);

        try (RichConnection connection = new RichConnection(
                dbSettings.server, dbSettings.domain,
                dbSettings.user, dbSettings.password,
                dbSettings.dbType
        )) {
            return new TablesInfoDto(connection.getTableNames(dbSettings.database));
        }
    }
}
