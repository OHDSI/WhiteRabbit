package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.springframework.stereotype.Service;

import java.io.File;

import static com.arcadia.whiteRabbitService.service.Constants.scanReportFileName;
import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.adapt;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.readAllBytes;

@AllArgsConstructor
@Service
public class WhiteRabbitFacade {

    private final WebSocketLogger webSocketLogger;

    @SneakyThrows
    public byte[] generateScanReport(DbSettingsDto dto) throws DbTypeNotSupportedException {
        DbSettings dbSettings = adapt(dto);

        SourceDataScan sourceDataScan = new SourceDataScanBuilder()
                .setSampleSize(dto.getSampleSize())
                .setScanValues(dto.isScanValues())
                .setMinCellCount(dto.getMinCellCount())
                .setMaxValues(dto.getMaxValues())
                .setCalculateNumericStats(dto.isCalculateNumericStats())
                .setNumericStatsSamplerSize(dto.getNumericStatsSamplerSize())
                .setLogger(webSocketLogger)
                .build();

        sourceDataScan.process(dbSettings, scanReportFileName);

        var reportFile = new File(scanReportFileName);
        var reportPath = reportFile.toPath();
        var reportBytes = readAllBytes(reportPath);

        delete(reportPath);

        return reportBytes;
    }
}
