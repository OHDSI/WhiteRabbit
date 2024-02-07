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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.databases.configuration.DbSettings;
import org.ohdsi.databases.configuration.DbType;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class SourceDataScanMySQLIT {

    @Container
    public static MySQLContainer<?> mySQLContainer = createMySQLContainer();

    @Test
    public void connectToDatabase() {
        // this is also implicitly tested by testSourceDataScan(), but having it fail separately helps identify problems quicker
        DbSettings dbSettings = getTestDbSettings();
        try (RichConnection richConnection = new RichConnection(dbSettings)) {
            // do nothing, connection will be closed automatically because RichConnection implements interface Closeable
        }
    }

    @Test
    public void testGetTableNames() {
        // this is also implicitly tested by testSourceDataScan(), but having it fail separately helps identify problems quicker
        DbSettings dbSettings = getTestDbSettings();
        List<String> tableNames = getTableNames(dbSettings);
        assertEquals(2, tableNames.size());
    }

    public static MySQLContainer<?> createMySQLContainer() {
        MySQLContainer<?> mySQLContainer = new MySQLContainer<>("mysql:8.2")
                .withUsername("root")
                .withPassword("test")
                .withEnv("MYSQL_ROOT_PASSWORD", "test")
                .withDatabaseName("test")
                //.withReuse(true)
                .withClasspathResourceMapping(
                        "scan_data",
                        "/var/lib/mysql-files", // this is the directory configured in mysql to be accessible for scripts/files
                        BindMode.READ_ONLY)
                .withInitScript("scan_data/create_data_mysql.sql");

        mySQLContainer.start();

        return mySQLContainer;
    }

    @Test
    void testSourceDataScan(@TempDir Path tempDir) throws IOException, URISyntaxException {
        Path outFile = tempDir.resolve("scanresult.xslx");
        URL referenceScanReport = SourceDataScanMySQLIT.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sql.xlsx");

        SourceDataScan sourceDataScan = ScanTestUtils.createSourceDataScan();
        DbSettings dbSettings = getTestDbSettings();

        sourceDataScan.process(dbSettings, outFile.toString());
        Files.copy(outFile, Paths.get("/var/tmp/ScanReport.xlsx"), StandardCopyOption.REPLACE_EXISTING);
        assertTrue(ScanTestUtils.scanResultsSheetMatchesReference(outFile, Paths.get(referenceScanReport.toURI()), DbType.MYSQL));
    }

    private List<String> getTableNames(DbSettings dbSettings) {
        try (RichConnection richConnection = new RichConnection(dbSettings)) {
            return richConnection.getTableNames(mySQLContainer.getDatabaseName());
        }
    }

    private DbSettings getTestDbSettings() {
        DbSettings dbSettings = new DbSettings();
        dbSettings.dbType = DbType.MYSQL;
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;
        dbSettings.server = mySQLContainer.getJdbcUrl();
        dbSettings.database = mySQLContainer.getDatabaseName();
        dbSettings.user = mySQLContainer.getUsername();
        dbSettings.password = mySQLContainer.getPassword();
        dbSettings.tables = getTableNames(dbSettings);

        return dbSettings;
    }
}
