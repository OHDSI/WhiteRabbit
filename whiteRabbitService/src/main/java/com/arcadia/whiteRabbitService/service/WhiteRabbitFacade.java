package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.*;
import com.arcadia.whiteRabbitService.service.error.DbTypeNotSupportedException;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.Logger;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.Future;

import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.*;
import static com.arcadia.whiteRabbitService.util.Base64Util.removeBase64Header;
import static com.arcadia.whiteRabbitService.util.FileUtil.*;
import static java.lang.String.format;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.readAllBytes;

@AllArgsConstructor
@Service
public class WhiteRabbitFacade {

    private final FakeDataService fakeDataService;

    @Async
    public Future<byte[]> generateScanReport(DbSettingsDto dto, Logger logger) throws FailedToScanException {
        try {
            DbSettings dbSettings = adaptDbSettings(dto);
            SourceDataScan sourceDataScan = createSourceDataScan(dto.getScanParams(), logger);
            String scanReportFileName = generateRandomFileName();

            sourceDataScan.process(dbSettings, scanReportFileName);

            return new AsyncResult<>(getScanReportBytes(scanReportFileName));
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new FailedToScanException(e.getCause());
        }
    }

    @Async
    public Future<byte[]> generateScanReport(FileSettingsDto dto, Logger logger) throws FailedToScanException {
        String directoryName = generateRandomDirectory();

        try {
            DbSettings dbSettings = adaptDelimitedTextFileSettings(dto, directoryName);
            SourceDataScan sourceDataScan = createSourceDataScan(dto.getScanParams(), logger);
            String scanReportFileName = generateRandomFileName();

            dto.getFilesToScan()
                    .forEach(fileToScanDto -> base64ToFile(
                            Paths.get(directoryName, fileToScanDto.getFileName()),
                            removeBase64Header(fileToScanDto.getBase64())
                    ));

            sourceDataScan.process(dbSettings, scanReportFileName);

            return new AsyncResult<>(getScanReportBytes(scanReportFileName));
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new FailedToScanException(e.getCause());
        } finally {
            deleteRecursive(Paths.get(directoryName));
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

    @Async
    public Future<Void> generateFakeData(FakeDataParamsDto dto, Logger logger) throws FailedToGenerateFakeData {
        return fakeDataService.generateFakeData(dto, logger);
    }

    private SourceDataScan createSourceDataScan(ScanParamsDto dto, Logger logger) {
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
    private byte[] getScanReportBytes(String scanReportFileName) {
        var reportFile = new File(scanReportFileName);
        var reportPath = reportFile.toPath();
        var reportBytes = readAllBytes(reportPath);

        delete(reportPath);

        return reportBytes;
    }
}
