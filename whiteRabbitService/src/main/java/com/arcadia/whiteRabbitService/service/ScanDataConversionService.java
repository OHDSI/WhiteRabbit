package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;

import java.util.concurrent.Future;

public interface ScanDataConversionService {
    Future<Void> runConversion(ScanDataConversion conversion);
}
