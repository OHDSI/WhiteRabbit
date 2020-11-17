package com.arcadia.whiteRabbitService.service;

import lombok.NoArgsConstructor;
import org.ohdsi.utilities.Logger;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;

@NoArgsConstructor
public class SourceDataScanBuilder {
    private int sampleSize;

    private boolean scanValues = false;

    private int minCellCount;

    private int maxValues;

    private boolean calculateNumericStats = false;

    private int numericStatsSamplerSize;

    private Logger logger;

    public SourceDataScanBuilder setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
        return this;
    }

    public SourceDataScanBuilder setScanValues(boolean scanValues) {
        this.scanValues = scanValues;
        return this;
    }

    public SourceDataScanBuilder setMinCellCount(int minCellCount) {
        this.minCellCount = minCellCount;
        return this;
    }

    public SourceDataScanBuilder setMaxValues(int maxValues) {
        this.maxValues = maxValues;
        return this;
    }

    public SourceDataScanBuilder setCalculateNumericStats(boolean calculateNumericStats) {
        this.calculateNumericStats = calculateNumericStats;
        return this;
    }

    public SourceDataScanBuilder setNumericStatsSamplerSize(int numericStatsSamplerSize) {
        this.numericStatsSamplerSize = numericStatsSamplerSize;
        return this;
    }

    public SourceDataScanBuilder setLogger(Logger logger) {
        this.logger = logger;
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

        return sourceDataScan;
    }
}
