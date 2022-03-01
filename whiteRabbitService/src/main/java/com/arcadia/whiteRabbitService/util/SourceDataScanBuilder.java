package com.arcadia.whiteRabbitService.util;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataParams;
import org.ohdsi.whiteRabbit.Logger;
import org.ohdsi.whiteRabbit.Interrupter;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;

public class SourceDataScanBuilder {
    private Integer sampleSize;
    private boolean scanValues = true;
    private Integer minCellCount;
    private Integer maxValues;
    private boolean calculateNumericStats = false;
    private Integer numericStatsSamplerSize;
    private Logger logger;
    private Interrupter interrupter;

    public SourceDataScanBuilder setSampleSize(Integer sampleSize) {
        this.sampleSize = sampleSize != null ? sampleSize : 100000; // Default value
        return this;
    }

    public SourceDataScanBuilder setScanValues(boolean scanValues) {
        this.scanValues = scanValues;
        return this;
    }

    public SourceDataScanBuilder setMinCellCount(Integer minCellCount) {
        this.minCellCount = minCellCount != null ? minCellCount : 5;
        return this;
    }

    public SourceDataScanBuilder setMaxValues(Integer maxValues) {
        this.maxValues = maxValues != null ? maxValues : 1000;
        return this;
    }

    public SourceDataScanBuilder setCalculateNumericStats(boolean calculateNumericStats) {
        this.calculateNumericStats = calculateNumericStats;
        return this;
    }

    public SourceDataScanBuilder setNumericStatsSamplerSize(Integer numericStatsSamplerSize) {
        this.numericStatsSamplerSize = numericStatsSamplerSize != null ? numericStatsSamplerSize : 100000;
        return this;
    }

    public SourceDataScanBuilder setLogger(Logger logger) {
        this.logger = logger;
        return this;
    }

    public SourceDataScanBuilder setInterrupter(Interrupter interrupter) {
        this.interrupter = interrupter;
        return this;
    }

    public SourceDataScan build() {
        final SourceDataScan sourceDataScan = new SourceDataScan();
        sourceDataScan.setSampleSize(sampleSize);
        sourceDataScan.setScanValues(scanValues);
        sourceDataScan.setMinCellCount(minCellCount);
        sourceDataScan.setMaxValues(maxValues);
        sourceDataScan.setCalculateNumericStats(calculateNumericStats);
        sourceDataScan.setNumStatsSamplerSize(numericStatsSamplerSize);
        sourceDataScan.setLogger(logger);
        sourceDataScan.setInterrupter(interrupter);

        return sourceDataScan;
    }

    public static SourceDataScan createSourceDataScan(ScanDataParams scanDataParams,
                                                      Logger logger,
                                                      Interrupter interrupter) {
        return new SourceDataScanBuilder()
                .setSampleSize(scanDataParams.getSampleSize())
                .setScanValues(scanDataParams.getScanValues())
                .setMinCellCount(scanDataParams.getMinCellCount())
                .setMaxValues(scanDataParams.getMaxValues())
                .setCalculateNumericStats(scanDataParams.getCalculateNumericStats())
                .setNumericStatsSamplerSize(scanDataParams.getNumericStatsSamplerSize())
                .setLogger(logger)
                .setInterrupter(interrupter)
                .build();
    }
}
