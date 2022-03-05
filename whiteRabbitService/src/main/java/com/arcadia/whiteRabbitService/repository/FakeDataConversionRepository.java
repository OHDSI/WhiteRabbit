package com.arcadia.whiteRabbitService.repository;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FakeDataConversionRepository extends JpaRepository<FakeDataConversion, Long> {
}
