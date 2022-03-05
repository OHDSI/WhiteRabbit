package com.arcadia.whiteRabbitService.repository;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataSettings;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FakeDataSettingsRepository extends JpaRepository<FakeDataSettings, Long> {
}
