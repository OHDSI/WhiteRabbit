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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.whiterabbit.WhiteRabbitMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SourceDataScanDatabricksIT {
    public static final String ENV_SERVER_LABEL = "DATABRICKS_WR_TEST_SERVER";
    public static final String ENV_HTTP_PATH_LABEL = "DATABRICKS_WR_TEST_HTTP_PATH";
    public static final String ENV_PERSONAL_ACCESS_TOKEN_LABEL = "DATABRICKS_WR_TEST_PERSONAL_ACCESS_TOKEN";
    public static final String ENV_CATALOG_LABEL = "DATABRICKS_WR_TEST_CATALOG";
    public static final String ENV_SCHEMA_LABEL = "DATABRICKS_WR_TEST_SCHEMA";

    static Logger logger = LoggerFactory.getLogger(SourceDataScanDatabricksIT.class);

    @Test
    void testProcessDatabricksFromIni(@TempDir Path tempDir) throws URISyntaxException, IOException {
        Assumptions.assumeTrue(new ScanTestUtils.PropertiesFileChecker("databricks.env"), "Databricks properties file not available");
        Charset charset = StandardCharsets.UTF_8;
        Path iniFile = tempDir.resolve("databricks.ini");
        URL iniTemplate = SourceDataScanDatabricksIT.class.getClassLoader().getResource("scan_data/databricks.ini.template");
        URL referenceScanReport = SourceDataScanDatabricksIT.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sql.xlsx");
        assert iniTemplate != null;
        String content = new String(Files.readAllBytes(Paths.get(iniTemplate.toURI())), charset);
        content = content.replace("%WORKING_FOLDER%", tempDir.toString())
                .replace("%DATABRICKS_SERVER%", ScanTestUtils.getPropertyOrFail(ENV_SERVER_LABEL))
                .replace("%DATABRICKS_HTTP_PATH%", ScanTestUtils.getPropertyOrFail(ENV_HTTP_PATH_LABEL))
                .replace("%DATABRICKS_PERSONAL_ACCESS_TOKEN%", ScanTestUtils.getPropertyOrFail(ENV_PERSONAL_ACCESS_TOKEN_LABEL))
                .replace("%DATABRICKS_CATALOG%", ScanTestUtils.getPropertyOrFail(ENV_CATALOG_LABEL))
                .replace("%DATABRICKS_SCHEMA%", ScanTestUtils.getPropertyOrFail(ENV_SCHEMA_LABEL));
        Files.write(iniFile, content.getBytes(charset));
        WhiteRabbitMain wrMain = new WhiteRabbitMain(true, new String[]{"-ini", iniFile.toAbsolutePath().toString()});
        assert referenceScanReport != null;
        assertTrue(ScanTestUtils.scanResultsSheetMatchesReference(tempDir.resolve("ScanReport.xlsx"), Paths.get(referenceScanReport.toURI()), DbType.DATABRICKS));
    }

    private static void execAndVerifyCommand(GenericContainer<?> container, String... command) throws IOException, InterruptedException {
        execAndVerifyCommand(container, 0, command);
    }

    private static void execAndVerifyCommand(GenericContainer<?> container, int expectedExitValue, String... command) throws IOException, InterruptedException {
        org.testcontainers.containers.Container.ExecResult result;

        result = container.execInContainer(command);
        if (result.getExitCode() != expectedExitValue) {
            logger.error("stdout: {}", result.getStdout());
            logger.error("stderr: {}", result.getStderr());
            // hide the password, if present, so it won't appear in logs (pragmatic)
            String message = ("Command failed: " + String.join(" ", command))
                    .replace(ScanTestUtils.getPropertyOrFail(ENV_PERSONAL_ACCESS_TOKEN_LABEL), "*****");
            assertEquals(expectedExitValue, result.getExitCode(), message);
        }
    }
}
