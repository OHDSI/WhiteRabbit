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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.databases.configuration.DbSettings;
import org.ohdsi.databases.configuration.DbType;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SourceDataScanBigQueryIT {

    @BeforeAll
    public static void startContainer() {
    }

    // Disabled: this test fails since the BigQuery JDBC jar is no longer included by default; but it could be handy when
    // testing/debugging BigQuery issues, so it is left in place
    //@Test
    void testSourceDataScan(@TempDir Path tempDir) throws IOException, URISyntaxException {
        Assumptions.assumeTrue(new ScanTestUtils.PropertiesFileChecker("bigquery.env"), "No BigQuery properties file present, skipping BigQuery test(s).");
        Path outFile = tempDir.resolve("scanresult.xlsx");
        URL referenceScanReport = SourceDataScanBigQueryIT.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sql.xlsx");

        SourceDataScan sourceDataScan = ScanTestUtils.createSourceDataScan();
        DbSettings dbSettings = getTestDbSettings();

        sourceDataScan.process(dbSettings, outFile.toString());

        Files.copy(outFile, Paths.get("/var/tmp/scanresults-bigquery.xlsx"), StandardCopyOption.REPLACE_EXISTING);
        assertTrue(ScanTestUtils.scanResultsSheetMatchesReference(outFile, Paths.get(referenceScanReport.toURI()), DbType.BIGQUERY));
    }

    private List<String> getTableNames(DbSettings dbSettings) {
        try (RichConnection richConnection = new RichConnection(dbSettings)) {
            return richConnection.getTableNames(ScanTestUtils.getPropertyOrFail("BIGQUERY_DATASET") );
        }
    }

    private DbSettings getTestDbSettings() {
        // TODO get settings from bigquery.env file
        DbSettings dbSettings = new DbSettings();
        dbSettings.dbType = DbType.BIGQUERY;
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;
        dbSettings.server = ScanTestUtils.getPropertyOrFail("BIGQUERY_PROJECT_ID");
        dbSettings.user = ScanTestUtils.getPropertyOrFail("BIGQUERY_ACCOUNT");
        dbSettings.password = ScanTestUtils.getPropertyOrFail("BIGQUERY_KEY_FILE");
        dbSettings.tables = getTableNames(dbSettings);
        dbSettings.database = ScanTestUtils.getPropertyOrFail("BIGQUERY_DATASET");
        dbSettings.domain = "";
        dbSettings.schema = "";

        return dbSettings;
    }
}
