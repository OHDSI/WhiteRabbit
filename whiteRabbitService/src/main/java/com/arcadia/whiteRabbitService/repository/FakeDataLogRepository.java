package com.arcadia.whiteRabbitService.repository;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface FakeDataLogRepository extends JpaRepository<FakeDataLog, Long> {
    List<FakeDataLog> findAllByFakeDataConversionId(Long conversionId);
}
