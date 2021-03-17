package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import org.ohdsi.databases.DbType;
import org.ohdsi.utilities.Logger;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.fakeDataGenerator.FakeDataGenerator;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Future;

import static com.arcadia.whiteRabbitService.util.Base64Util.removeBase64Header;
import static com.arcadia.whiteRabbitService.util.FileUtil.*;

@Service
public class FakeDataService {

    private final DbSettings dbSettings = new DbSettings();

    /** Init fake data connection string **/
    public FakeDataService() {
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;
        dbSettings.dbType = DbType.POSTGRESQL;
        dbSettings.server = "10.110.1.7:5431/cdm_souffleur";
        dbSettings.database = "public";
        dbSettings.user = "postgres";
        dbSettings.password = "5eC_DkMr^3";
    }

    public String generateFakeData(FakeDataParamsDto dto, Logger logger) throws FailedToGenerateFakeData {
        String directoryName = generateRandomDirectory();
        String fileName = generateRandomFileName();
        String schemaName = dto.getSchema() != null ? dto.getSchema() : "public"; // Default schema name
        Path filePath = Paths.get(directoryName, fileName);

        try {
            base64ToFile(
                    filePath,
                    removeBase64Header(dto.getScanReportBase64())
            );

            FakeDataGenerator process = new FakeDataGenerator();
            process.setLogger(logger);

            process.generateData(
                    dbSettings,
                    dto.getMaxRowCount(),
                    filePath.toString(),
                    null, // Not needed, it need if generate fake data to delimited text file
                    dto.getDoUniformSampling(),
                    schemaName,
                    false // Tables are created when the report is uploaded to python service
            );

            return "Fake data successfully generated";
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new FailedToGenerateFakeData(e.getCause());
        } finally {
            deleteRecursive(Paths.get(directoryName));
        }
    }
}
