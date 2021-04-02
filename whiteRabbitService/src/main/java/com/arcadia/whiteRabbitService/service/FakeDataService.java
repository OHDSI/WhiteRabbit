package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.service.error.DbTypeNotSupportedException;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import org.ohdsi.utilities.Logger;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.fakeDataGenerator.FakeDataGenerator;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.adaptDbSettings;
import static com.arcadia.whiteRabbitService.util.Base64Util.removeBase64Header;
import static com.arcadia.whiteRabbitService.util.FileUtil.*;

@Service
public class FakeDataService {

    public String generateFakeData(FakeDataParamsDto dto, Logger logger) throws FailedToGenerateFakeData, DbTypeNotSupportedException {
        String directoryName = generateRandomDirectory();
        String fileName = generateRandomFileName();
        String schemaName = dto.getDbSettings().getSchema();
        DbSettings dbSettings = adaptDbSettings(dto.getDbSettings());
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
