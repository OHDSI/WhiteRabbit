package com.arcadia.whiteRabbitService.repository;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataResult;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ScanDataResultRepository extends JpaRepository<ScanDataResult, Long> {
    Optional<ScanDataResult> findByScanDataConversionId(Long scanDataConversionId);
}
