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
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.configuration.DbSettings;
import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.databases.RichConnection;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SourceDataScanOracleIT {

    private final static String USER_NAME = "test_user";
    private final static String SCHEMA_NAME = USER_NAME;

    /*
     * Since the database is only read, setting it up once suffices.
     *
     * Note that the init script is read locally, but accesses the CSV files from
     * the resource mapped into the container.
     *
     * The data used in this test are actually OMOP data. One reason for this is convenience: the DDL
     * for this data is know and could simply be copied instead of composed.
     * Also, for the technical correctness of WhiteRabbit (does it open the database, get the table
     * names and scan those tables), the actual nature of the source data does not matter.
     */
    @Container
    public static OracleContainer oracleContainer = new OracleContainer("gvenzl/oracle-xe:11.2.0.2-slim-faststart")
            .withReuse(true)
            .usingSid()
            .withUsername(USER_NAME)
            .withPassword("test_password")
            .withDatabaseName("testDB")
            .withInitScript("scan_data/create_data_oracle.sql");

    @BeforeAll
    public static void startContainer() {
        oracleContainer.start();
    }

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
    @Test
    void testSourceDataScan(@TempDir Path tempDir) throws IOException, URISyntaxException {
        loadData();
        Path outFile = tempDir.resolve("scanresult.xslx");
        URL referenceScanReport = SourceDataScanOracleIT.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sql.xlsx");

        SourceDataScan sourceDataScan = ScanTestUtils.createSourceDataScan();
        DbSettings dbSettings = getTestDbSettings();

        sourceDataScan.process(dbSettings, outFile.toString());
        assertTrue(ScanTestUtils.scanResultsSheetMatchesReference(outFile, Paths.get(referenceScanReport.toURI()), DbType.ORACLE));
    }

    private void loadData() {
        insertDataFromCsv("person");
        insertDataFromCsv("cost");
    }

    private void insertDataFromCsv(String tableName) {
        DbSettings dbSettings = getTestDbSettings();
        try (RichConnection richConnection = new RichConnection(dbSettings)) {
            try (BufferedReader reader = new BufferedReader(getResourcePath(tableName))) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    String[] values = line.split(",");
                    if (line.endsWith(",")) {
                        values = Arrays.copyOf(values, values.length + 1);
                        values[values.length - 1] = "";
                    }
                    // Oracle INSERT needs quotes around the values
                    String insertSql = String.format("INSERT INTO %s.%s VALUES('%s');", dbSettings.database, tableName, String.join("','", values));
                    richConnection.execute(insertSql);
                }
            } catch (IOException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private InputStreamReader getResourcePath(String tableName) throws URISyntaxException, IOException {
        String resourceName = String.format("scan_data/%s-no-header.csv", tableName);

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).toURI());
        return new InputStreamReader(Files.newInputStream(file.toPath()));
    }

    private List<String> getTableNames(DbSettings dbSettings) {
        try (RichConnection richConnection = new RichConnection(dbSettings)) {
            return richConnection.getTableNames(SCHEMA_NAME);
        }
    }

    private DbSettings getTestDbSettings() {
        DbSettings dbSettings = new DbSettings();
        dbSettings.dbType = DbType.ORACLE;
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;
        //dbSettings.server = oracleContainer.getJdbcUrl();
        dbSettings.server = String.format("%s:%s/%s", oracleContainer.getHost(), oracleContainer.getOraclePort(), "XE");
        if (dbSettings.server.toLowerCase().contains("thin")) {
            dbSettings.server = dbSettings.server.replace("/test", ":test").replace("@", "");
        }
        dbSettings.user = oracleContainer.getUsername();
        dbSettings.password = oracleContainer.getPassword();
        dbSettings.tables = getTableNames(dbSettings);
        dbSettings.database = SCHEMA_NAME;

        return dbSettings;
    }
}
