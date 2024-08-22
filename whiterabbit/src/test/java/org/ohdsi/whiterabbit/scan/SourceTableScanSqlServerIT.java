/*******************************************************************************
 * Copyright 2023 Observational Health Data Sciences and Informatics & The Hyve
 *
 * This file is part of WhiteRabbit
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.whiterabbit.scan;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.databases.configuration.DbSettings;
import org.ohdsi.databases.configuration.DbType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;

/*
  All this test does is:
  - connect to the database
  - get a list of tables available and verify that there are more than 0
  No actual WhiteRabbit scan of the database is performed.
 */
class SourceTableScanSqlServerIT {
    Logger logger = LoggerFactory.getLogger(SourceTableScanSqlServerIT.class);
    private static MSSQLServerContainer<?> mssqlserver;

    @BeforeAll
    static void setUp() {
        mssqlserver = new MSSQLServerContainer<>(DockerImageName.parse("mcr.microsoft.com/mssql/server:2019-latest"))
                .acceptLicense()
                .withEnv("MSSQL_PID", "Developer")
                .withExposedPorts(1433);

        mssqlserver.start();
    }

    //@Test
    void testGetTableNames() {
        DbSettings dbSettings = getTestDbSettings();
        List<String> tableNames = getTableNames(dbSettings);
        assertFalse(tableNames.isEmpty(), "Some tables were expected to exist");
    }

    private List<String> getTableNames(DbSettings dbSettings) {
        try (RichConnection richConnection = new RichConnection(dbSettings)) {
            return richConnection.getTableNames(dbSettings.database);
        }
    }

    private DbSettings getTestDbSettings() {
        DbSettings dbSettings = new DbSettings();
        dbSettings.dbType = DbType.SQL_SERVER;
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;
        dbSettings.server = mssqlserver.getHost() + ":" + mssqlserver.getFirstMappedPort();
        dbSettings.user = mssqlserver.getUsername();
        dbSettings.password = mssqlserver.getPassword();
        dbSettings.database = ""; // it will return some system tables, which is goo enough for this test
        dbSettings.domain = "";
        dbSettings.schema = "";

        return dbSettings;
    }
}
