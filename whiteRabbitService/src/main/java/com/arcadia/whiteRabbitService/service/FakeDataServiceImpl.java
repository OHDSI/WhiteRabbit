//package com.arcadia.whiteRabbitService.service;
//
//import com.arcadia.whiteRabbitService.dto.FakeDataParamsDto;
//import com.arcadia.whiteRabbitService.service.error.FailedToGenerateFakeData;
//import org.ohdsi.utilities.WRLogger;
//import org.ohdsi.whiteRabbit.DbSettings;
//import org.ohdsi.whiteRabbit.fakeDataGenerator.FakeDataGenerator;
//import org.springframework.stereotype.Service;
//
//import java.nio.file.Path;
//
//import static com.arcadia.whiteRabbitService.util.DbSettingsAdapter.adaptDbSettings;
//import static com.arcadia.whiteRabbitService.util.FileUtil.deleteRecursive;
//
//@Service
//public class FakeDataServiceImpl implements FakeDataService {
//
//    @Override
//    public void generateFakeData(FakeDataParamsDto dto, WRLogger logger) throws FailedToGenerateFakeData {
//        try {
//            DbSettings dbSettings = adaptDbSettings(dto.getDbSettings());
//            FakeDataGenerator process = new FakeDataGenerator();
//            process.setLogger(logger);
//
//            process.generateData(
//                    dbSettings,
//                    dto.getMaxRowCount(),
//                    dto.getScanReportFileName(),
//                    null, // Not needed, it need if generate fake data to delimited text file
//                    dto.getDoUniformSampling(),
//                    dto.getDbSettings().getSchema(),
//                    false // False - Tables are created when the report is uploaded to python service
//            );
//        } catch (Exception e) {
//            if (e instanceof InterruptedException) {
//                logger.cancel(e.getMessage());
//            } else {
//                logger.error(e.getMessage());
//            }
//            FailedToGenerateFakeData exception = new FailedToGenerateFakeData(e);
//            logger.failed(exception.getMessage());
//            throw exception;
//        } finally {
//            deleteRecursive(Path.of(dto.getDirectory()));
//        }
//    }
//}
