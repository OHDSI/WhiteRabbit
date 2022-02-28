package com.arcadia.whiteRabbitService.repository;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataResult;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ScanDataResultRepository extends JpaRepository<ScanDataResult, Long> {
    ScanDataResult findByScanDataConversionId(Long scanDataConversionId);
}
