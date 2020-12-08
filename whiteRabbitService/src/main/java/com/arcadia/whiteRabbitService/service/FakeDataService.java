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

    public FakeDataService() {
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;
        dbSettings.dbType = DbType.POSTGRESQL;
        dbSettings.server = "10.110.1.7/testdb";
        dbSettings.database = "testdb";
        dbSettings.user = "postgres";
        dbSettings.password = "postgres";
    }

    @Async
    public Future<Void> generateFakeData(FakeDataParamsDto dto, Logger logger) throws FailedToGenerateFakeData {
        String directoryName = generateRandomDirectory();
        String fileName = generateRandomFileName();
        String schemaName = dto.getSchemaName() != null ? dto.getSchemaName() : "public"; // default schema name
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
                    null, // not needed, it need if generate fake data in delimited text file
                    dto.getDoUniformSampling(),
                    schemaName,
                    false
            );

            return new AsyncResult<>(null);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new FailedToGenerateFakeData(e.getCause());
        } finally {
            deleteRecursive(Paths.get(directoryName));
        }
    }
}
