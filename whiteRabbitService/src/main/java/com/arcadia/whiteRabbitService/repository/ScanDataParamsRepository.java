package com.arcadia.whiteRabbitService.repository;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataParams;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ScanDataParamsRepository extends JpaRepository<ScanDataParams, Long> {
}
