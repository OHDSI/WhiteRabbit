package com.arcadia.whiteRabbitService.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
public class FakeDataDbConfig {
    @Value("${fake-data-db.db-type}")
    private String dbType;

    @Value("${fake-data-db.server}")
    private String server;

    @Value("${fake-data-db.port}")
    private Integer port;

    @Value("${fake-data-db.database}")
    private String database;

    @Value("${fake-data-db.user}")
    private String user;

    @Value("${fake-data-db.password}")
    private String password;
}
