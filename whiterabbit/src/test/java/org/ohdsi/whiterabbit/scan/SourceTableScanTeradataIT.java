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
import org.ohdsi.databases.RichConnection;
import org.ohdsi.databases.configuration.DbSettings;
import org.ohdsi.databases.configuration.DbType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/*
  This test class is intended to verify an update to the Teradata JDBC driver, as it can only be tested
  with a local test instance of Teradata that can be obtained from https://downloads.teradata.com/download/database/teradata-express/vmware

  When this test fails, that should be due to a new/untested version of the Teradata JDBC driver. When that happens,
  test with the instructions below, and after success, include the new version in the VALIDATED_VERSIONS List below.

  A VirtualBox appliance containing Teradata Vantage 17.20 was used for the most recent validation.
  Global steps to perform before running this test:
  1. Download the Vantage Express 17.20 VirtualBox Open Virtual Appliance (OVA) (you may need to register)
  2. Import the downloaded file into VirtualBox (download & install VirtualBox first if needed)
  3. Start the appliance and wait for the database to become available
  See here for instructions, including how to verify that the database is available: https://quickstarts.teradata.com/getting.started.vbox.html

  Then, enable this test and run it. The default credentials setup by getTestDBSettings() are expected to work.

  All this test does is:
  - connect to the database
  - get a list of tables available and verify that there are more than 0
  No actual WhiteRabbit scan of the database is performed.
 */
class SourceTableScanTeradataIT {

    private final static List<String> VALIDATED_VERSIONS = Arrays.asList("16.20.00.13", "17.20.00.15", "20.00.00.16");

    Logger logger = LoggerFactory.getLogger(SourceTableScanTeradataIT.class);

    // Teradata does not allow redistribution of the JDBC driver, this test can only be run for local test purposes
    //@Test
    void testGetTableNames() throws ClassNotFoundException {
        if (!VALIDATED_VERSIONS.contains(getVersion())) {
            logger.warn("Version {} of Teradata JDBC driver was not tested before, please make sure a local instance of Teradata is available," +
                    "See instructions in the class that generates this warning.", getVersion());
            DbSettings dbSettings = getTestDbSettings();
            List<String> tableNames = getTableNames(dbSettings);
            assertFalse(tableNames.isEmpty(), "Some tables were expected to exist");
        }
    }

    private List<String> getTableNames(DbSettings dbSettings) {
        try (RichConnection richConnection = new RichConnection(dbSettings)) {
            return richConnection.getTableNames(dbSettings.database);
        }
    }

    private DbSettings getTestDbSettings() {
        DbSettings dbSettings = new DbSettings();
        dbSettings.dbType = DbType.TERADATA;
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;
        dbSettings.server = "127.0.0.1";
        dbSettings.user = "dbc";
        dbSettings.password = "dbc";
        dbSettings.database = "dbc";
        dbSettings.domain = "";
        dbSettings.schema = "";

        return dbSettings;
    }

    private synchronized String getVersion() throws ClassNotFoundException {
        String version = null;

        // try to load from maven properties first
        try {
            Properties p = new Properties();
            InputStream is = getClass().getResourceAsStream("/META-INF/maven/com.teradata.jdbc/terajdbc/pom.properties");
            if (is != null) {
                p.load(is);
                version = p.getProperty("version", "");
            }
        } catch (Exception e) {
            // ignore
        }

        // fallback to using Java API
        if (version == null) {
            Package aPackage = Class.forName("com.teradata.jdbc.TeraDriver").getPackage();
            if (aPackage != null) {
                version = aPackage.getImplementationVersion();
                if (version == null) {
                    version = aPackage.getSpecificationVersion();
                }
            }
        }

        if (version == null) {
            // we could not compute the version so use a blank
            version = "";
        }

        return version;
    }
}
