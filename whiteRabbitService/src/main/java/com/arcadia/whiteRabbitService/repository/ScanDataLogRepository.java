package com.arcadia.whiteRabbitService.repository;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ScanDataLogRepository extends JpaRepository<ScanDataLog, Long> {
    List<ScanDataLog> findAllByScanDataConversionId(Long scanDataConversionId);
}
