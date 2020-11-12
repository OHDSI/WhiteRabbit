package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.*;
import com.arcadia.whiteRabbitService.service.error.DbTypeNotSupportedException;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.Logger;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Path;

import static com.arcadia.whiteRabbitService.service.Constants.scanReportFileName;
import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.adaptDbSettings;
import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.adaptDelimitedTextFileSettings;
import static com.arcadia.whiteRabbitService.util.FileUtil.base64ToFile;
import static com.arcadia.whiteRabbitService.util.MediaType.getBase64HeaderForDelimitedTextFile;
import static java.lang.String.format;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.readAllBytes;
import static java.util.stream.Collectors.toList;

@AllArgsConstructor
@Service
public class WhiteRabbitFacade {

    public byte[] generateScanReport(DbSettingsDto dto, Logger logger) throws FailedToScanException {
        try {
            DbSettings dbSettings = adaptDbSettings(dto);
            SourceDataScan sourceDataScan = createSourceDataScan(dto.getScanParameters(), logger);

            sourceDataScan.process(dbSettings, scanReportFileName);

            return getScanReportBytes();

        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new FailedToScanException(e.getCause());
        }
    }

    public byte[] generateScanReport(DelimitedTextFileSettingsDto dto, Logger logger) throws FailedToScanException {
        try {
            DbSettings dbSettings = adaptDelimitedTextFileSettings(dto);
            SourceDataScan sourceDataScan = createSourceDataScan(dto.getScanParameters(), logger);

            var delimitedFilePaths = dto.getFilesToScan()
                    .stream()
                    .map(fileToScanDto -> base64ToFile(
                            fileToScanDto.getFileName(),
                            fileToScanDto.getBase64().substring(getBase64HeaderForDelimitedTextFile(dbSettings.sourceType).length())
                    ))
                    .collect(toList());

            sourceDataScan.process(dbSettings, scanReportFileName);

            for (Path path : delimitedFilePaths) {
                delete(path);
            }

            return getScanReportBytes();

        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new FailedToScanException(e.getCause());
        }
    }

    public TestConnectionDto testConnection(DbSettingsDto dto) {
        try {
            DbSettings dbSettings = adaptDbSettings(dto);
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
        DbSettings dbSettings = adaptDbSettings(dto);

        try (RichConnection connection = new RichConnection(
                dbSettings.server, dbSettings.domain,
                dbSettings.user, dbSettings.password,
                dbSettings.dbType
        )) {
            return new TablesInfoDto(connection.getTableNames(dbSettings.database));
        }
    }

    private SourceDataScan createSourceDataScan(ScanParametersDto dto, Logger logger) {
        return new SourceDataScanBuilder()
                .setSampleSize(dto.getSampleSize())
                .setScanValues(dto.getScanValues())
                .setMinCellCount(dto.getMinCellCount())
                .setMaxValues(dto.getMaxValues())
                .setCalculateNumericStats(dto.getCalculateNumericStats())
                .setNumericStatsSamplerSize(dto.getNumericStatsSamplerSize())
                .setLogger(logger)
                .build();
    }

    @SneakyThrows
    private byte[] getScanReportBytes() {
        var reportFile = new File(scanReportFileName);
        var reportPath = reportFile.toPath();
        var reportBytes = readAllBytes(reportPath);

        delete(reportPath);

        return reportBytes;
    }
}
