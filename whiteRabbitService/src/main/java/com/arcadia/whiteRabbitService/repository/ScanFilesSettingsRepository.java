package com.arcadia.whiteRabbitService.repository;

import com.arcadia.whiteRabbitService.model.scandata.ScanFilesSettings;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ScanFilesSettingsRepository extends JpaRepository<ScanFilesSettings, Long> {
}
