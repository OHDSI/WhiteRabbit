package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataParams;
import com.arcadia.whiteRabbitService.model.scandata.ScanDbSettings;
import com.arcadia.whiteRabbitService.util.DbSettingsAdapter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.ohdsi.databases.DbType;
import org.ohdsi.whiteRabbit.DbSettings;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DbSettingsAdapterTest {

    @SneakyThrows
    @Test
    void testAdaptFields() {
        ScanDbSettings settings = createTestDbSettings("Sql Server", 1433);

        DbSettings dbSettings = DbSettingsAdapter.adaptDbSettings(settings);

        assertAll("Should be equals all string and primitive fields",
                () -> assertEquals(settings.getUser(), dbSettings.user),
                () -> assertEquals(settings.getPassword(), dbSettings.password),
                () -> assertEquals(settings.getDatabase(), dbSettings.database),
                () -> assertEquals(String.format("%s:%s", settings.getServer(), settings.getPort()), dbSettings.server)
        );
    }

    @SneakyThrows
    @Test
    void testAdaptMsSqlDbType() {
        ScanDbSettings settings = createTestDbSettings("SQL SERVER", 1433);
        DbSettings wrSettings = DbSettingsAdapter.adaptDbSettings(settings);

        assertEquals(wrSettings.dbType, DbType.MSSQL);
    }

    @SneakyThrows
    @Test
    void testAdaptPostgreDbType() {
        ScanDbSettings settings = createTestDbSettings("postgresql", 5432);
        DbSettings wrSettings = DbSettingsAdapter.adaptDbSettings(settings);

        assertEquals(wrSettings.dbType, DbType.POSTGRESQL);
    }

    public static ScanDbSettings createTestDbSettings(String dbType, int port) {
        String server = "822JNJ16S03V";
        String database = "CPRD";
        String user = "cdm_builder";
        String password = "builder1!";
        String tablesToScan = "dbo.lookup,dbo.medical";
        String schema = "";

        int sampleSize = (int) 100e3;
        int minCellCount = 5;
        int maxValues = 1000;
        int numericStatsSamplerSize = 500;

        ScanDataParams params = ScanDataParams.builder()
                .scanValues(true)
                .sampleSize(sampleSize)
                .minCellCount(minCellCount)
                .maxValues(maxValues)
                .calculateNumericStats(false)
                .numericStatsSamplerSize(numericStatsSamplerSize)
                .build();

        return ScanDbSettings
                .builder()
                .dbType(dbType)
                .user(user)
                .password(password)
                .database(database)
                .server(server)
                .port(port)
                .schema(schema)
                .tablesToScan(tablesToScan)
                .scanDataParams(params)
                .build();
    }
}
