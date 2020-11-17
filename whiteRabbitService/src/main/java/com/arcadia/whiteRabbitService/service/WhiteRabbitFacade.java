package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.*;
import com.arcadia.whiteRabbitService.service.error.DbTypeNotSupportedException;
import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.Logger;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.fakeDataGenerator.FakeDataGenerator;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Paths;

import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.adaptDbSettings;
import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.adaptDelimitedTextFileSettings;
import static com.arcadia.whiteRabbitService.util.FileUtil.*;
import static com.arcadia.whiteRabbitService.util.MediaTypeUtil.getBase64HeaderForDelimitedTextFile;
import static java.lang.String.format;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.readAllBytes;

@AllArgsConstructor
@Service
public class WhiteRabbitFacade {

    public byte[] generateScanReport(DbSettingsDto dto, Logger logger) throws FailedToScanException {
        try {
            DbSettings dbSettings = adaptDbSettings(dto);
            SourceDataScan sourceDataScan = createSourceDataScan(dto.getScanParameters(), logger);
            String scanReportFileName = generateRandomFileName();

            sourceDataScan.process(dbSettings, scanReportFileName);

            return getScanReportBytes(scanReportFileName);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new FailedToScanException(e.getCause());
        }
    }

    public byte[] generateScanReport(DelimitedTextFileSettingsDto dto, Logger logger) throws FailedToScanException {
        try {
            String directoryName = generateRandomDirectory();
            DbSettings dbSettings = adaptDelimitedTextFileSettings(dto, directoryName);
            SourceDataScan sourceDataScan = createSourceDataScan(dto.getScanParameters(), logger);
            String scanReportFileName = generateRandomFileName();

            dto.getFilesToScan().forEach(fileToScanDto -> base64ToFile(
                    Paths.get(directoryName, fileToScanDto.getFileName()),
                    fileToScanDto.getBase64().substring(getBase64HeaderForDelimitedTextFile(dbSettings.sourceType).length())
            ));

            sourceDataScan.process(dbSettings, scanReportFileName);

            deleteRecursive(Paths.get(directoryName));

            return getScanReportBytes(scanReportFileName);
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

    @SneakyThrows
    public void generateFakeData(ParamsForFakeDataGenerationDto dto) {
        DbSettings dbSettings = adaptDbSettings(dto.getDbSettings());

        String directoryName = generateRandomDirectory();
        String fileName = generateRandomFileName();

        base64ToFile(Paths.get(directoryName, fileName), dto.getScanReportBase64());

        FakeDataGenerator process = new FakeDataGenerator();
        process.generateData(
                dbSettings,
                dto.getMaxRowCount(),
                fileName, directoryName,
                dto.isDoUniformSampling()
        );

        deleteRecursive(Paths.get(directoryName));
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
