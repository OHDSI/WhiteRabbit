package com.arcadia.whiteRabbitService.repository;

import com.arcadia.whiteRabbitService.model.scandata.ScanDbSettings;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ScanDbSettingsRepository extends JpaRepository<ScanDbSettings, Long> {
}
