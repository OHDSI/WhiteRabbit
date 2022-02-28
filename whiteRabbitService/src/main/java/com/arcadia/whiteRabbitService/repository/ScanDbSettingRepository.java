package com.arcadia.whiteRabbitService.repository;

import com.arcadia.whiteRabbitService.model.scandata.ScanDbSetting;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ScanDbSettingRepository extends JpaRepository<ScanDbSetting, Long> {
}
