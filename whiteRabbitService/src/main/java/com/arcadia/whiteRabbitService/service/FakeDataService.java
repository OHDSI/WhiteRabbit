package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
import com.arcadia.whiteRabbitService.service.error.DbTypeNotSupportedException;
import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
import lombok.AllArgsConstructor;
import org.ohdsi.utilities.Logger;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.fakeDataGenerator.FakeDataGenerator;
import org.springframework.stereotype.Service;

import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.adaptDbSettings;

@Service
@AllArgsConstructor
public class FakeDataService {

    private final StorageService storageService;

    public void generateFakeData(FakeDataParamsDto dto, Logger logger) throws FailedToGenerateFakeData, DbTypeNotSupportedException {
        DbSettings dbSettings = adaptDbSettings(dto.getDbSettings());

        try {
            FakeDataGenerator process = new FakeDataGenerator();
            process.setLogger(logger);

            process.generateData(
                    dbSettings,
                    dto.getMaxRowCount(),
                    dto.getScanReportFileName(),
                    null, // Not needed, it need if generate fake data to delimited text file
                    dto.getDoUniformSampling(),
                    dto.getDbSettings().getSchema(),
                    false // Tables are created when the report is uploaded to python service
            );
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new FailedToGenerateFakeData(e.getCause());
        } finally {

            storageService.delete(dto.getScanReportFileName());
        }
    }
}
