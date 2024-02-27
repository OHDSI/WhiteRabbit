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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.whiterabbit.WhiteRabbitMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class SourceDataScanSnowflakeIT {

    public final static String SNOWFLAKE_ACCOUNT_ENVIRONMENT_VARIABLE = "SNOWFLAKE_WR_TEST_ACCOUNT";
    static Logger logger = LoggerFactory.getLogger(SourceDataScanSnowflakeIT.class);

    final static String CONTAINER_DATA_PATH = "/scan_data";
    @Container
    public static GenericContainer<?> testContainer;

    @BeforeEach
    public void setUp() {
        Assumptions.assumeTrue(new ScanTestUtils.PropertiesFileChecker("snowflake.env"), "Snowflake system properties file not available");
        try {
            testContainer = createPythonContainer();
            prepareSnowflakeTestData(testContainer);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Creating python container failed.");
        }
    }

    @Test
    void testProcessSnowflakeFromIni(@TempDir Path tempDir) throws URISyntaxException, IOException {
        Assumptions.assumeTrue(new ScanTestUtils.PropertiesFileChecker("snowflake.env"), "Snowflake system properties file not available");
        Charset charset = StandardCharsets.UTF_8;
        Path iniFile = tempDir.resolve("snowflake.ini");
        URL iniTemplate = SourceDataScanSnowflakeIT.class.getClassLoader().getResource("scan_data/snowflake.ini.template");
        URL referenceScanReport = SourceDataScanSnowflakeIT.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sql.xlsx");
        assert iniTemplate != null;
        String content = new String(Files.readAllBytes(Paths.get(iniTemplate.toURI())), charset);
        content = content.replace("%WORKING_FOLDER%", tempDir.toString())
                .replace("%SNOWFLAKE_ACCOUNT%", ScanTestUtils.getPropertyOrFail("SNOWFLAKE_WR_TEST_ACCOUNT"))
                .replace("%SNOWFLAKE_USER%", ScanTestUtils.getPropertyOrFail("SNOWFLAKE_WR_TEST_USER"))
                .replace("%SNOWFLAKE_PASSWORD%", ScanTestUtils.getPropertyOrFail("SNOWFLAKE_WR_TEST_PASSWORD"))
                .replace("%SNOWFLAKE_WAREHOUSE%", ScanTestUtils.getPropertyOrFail("SNOWFLAKE_WR_TEST_WAREHOUSE"))
                .replace("%SNOWFLAKE_DATABASE%", ScanTestUtils.getPropertyOrFail("SNOWFLAKE_WR_TEST_DATABASE"))
                .replace("%SNOWFLAKE_SCHEMA%", ScanTestUtils.getPropertyOrFail("SNOWFLAKE_WR_TEST_SCHEMA"));
        Files.write(iniFile, content.getBytes(charset));
        WhiteRabbitMain wrMain = new WhiteRabbitMain(true, new String[]{"-ini", iniFile.toAbsolutePath().toString()});
        assert referenceScanReport != null;
        assertTrue(ScanTestUtils.scanResultsSheetMatchesReference(tempDir.resolve("ScanReport.xlsx"), Paths.get(referenceScanReport.toURI()), DbType.SNOWFLAKE));
    }

    static void prepareSnowflakeTestData(GenericContainer<?> container) throws IOException, InterruptedException {
        ScanTestUtils.PropertiesFileChecker checker = new ScanTestUtils.PropertiesFileChecker("snowflake.env");
        if (checker.getAsBoolean()) {
            prepareSnowflakeTestData(container, new ScanTestUtils.PropertyReader());
        }
    }

    static void prepareSnowflakeTestData(GenericContainer<?> container, ScanTestUtils.ReaderInterface reader) throws IOException, InterruptedException {
        // snowsql is used for initializing the database

        // add some packages needed for the installation of snowsql
        execAndVerifyCommand(container, "/bin/sh", "-c", "apt update; apt -y install wget unzip");
        // download snowsql
        execAndVerifyCommand(container, "/bin/bash", "-c",
                "wget -q https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.29-linux_x86_64.bash;");
        // install snowsql
        execAndVerifyCommand(container, "/bin/bash", "-c",
                "echo -e \"/tmp\\nN\" | bash snowsql-1.2.29-linux_x86_64.bash ");

        // run the sql script needed to initialize the test data
        execAndVerifyCommand(container, "/bin/bash", "-c",
                String.format("(cd %s; SNOWSQL_PWD='%s' /tmp/snowsql -a %s -u %s -d %s -s %s -f %s/create_data_snowflake.sql)",
                        CONTAINER_DATA_PATH,
                        reader.getOrFail("SNOWFLAKE_WR_TEST_PASSWORD"),
                        reader.getOrFail(SNOWFLAKE_ACCOUNT_ENVIRONMENT_VARIABLE),
                        reader.getOrFail("SNOWFLAKE_WR_TEST_USER"),
                        reader.getOrFail("SNOWFLAKE_WR_TEST_DATABASE"),
                        reader.getOrFail("SNOWFLAKE_WR_TEST_SCHEMA"),
                        CONTAINER_DATA_PATH
                        ));
    }

    public static GenericContainer<?> createPythonContainer() throws IOException, InterruptedException {
        GenericContainer<?> testContainer = new GenericContainer<>(DockerImageName.parse("ubuntu:22.04"))
                .withCommand("/bin/sh", "-c", "tail -f /dev/null") // keeps the container running until it is explicitly stopped
                .withClasspathResourceMapping(
                        "scan_data",
                        CONTAINER_DATA_PATH,
                        BindMode.READ_ONLY);

        testContainer.start();

        return testContainer;
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
                    .replace(ScanTestUtils.getPropertyOrFail("SNOWFLAKE_WR_TEST_PASSWORD"), "*****")
                    .replace(ScanTestUtils.getEnvOrFail("SNOWFLAKE_WR_TEST_PASSWORD"), "*****");
            assertEquals(expectedExitValue, result.getExitCode(), message);
        }
    }
}
