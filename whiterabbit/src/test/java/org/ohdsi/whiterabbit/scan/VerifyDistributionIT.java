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

import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.DBConnector;
import org.ohdsi.databases.configuration.DbType;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.ohdsi.whiterabbit.scan.SourceDataScanSnowflakeIT.*;

/**
 * Intent: "deploy" the distributed application in a docker container (TestContainer) containing a Java runtime
 * of a specified version, and runs a test of WhiteRabbit that aim to verify that the distribution is complete,
 * i.e. no dependencies are missing. A data for a scan on csv files is used to run whiteRabbit.
 *
 * Note that this does not test any of the JDBC driver dependencies, unless these databases are actually used.
 */
@Disabled   // tests cause issues of unknown nature, and will need to be reviewed once the project
            // moves to Java 17 as the lowest supported version
public class VerifyDistributionIT {

    @TempDir
    static Path tempDir;

    private static final String WORKDIR_IN_CONTAINER = "/whiterabbit";
    private static final String APPDIR_IN_CONTAINER = "/app";

    @Test
    void testDistributionWithJava8() throws IOException, URISyntaxException, InterruptedException {
        testWhiteRabbitInContainer("eclipse-temurin:8", "openjdk version \"1.8.");
    }

    @Test
    void testDistributionWithJava11() throws IOException, URISyntaxException, InterruptedException {
        testWhiteRabbitInContainer("eclipse-temurin:11", "openjdk version \"11.0.");
    }

    @Test
    void testDistributionWithJava17() throws IOException, URISyntaxException, InterruptedException {
        testWhiteRabbitInContainer("eclipse-temurin:17", "openjdk version \"17.0.");
    }

    @Test
    void verifyAllJDBCDriversLoadable() throws IOException, InterruptedException {
        try (GenericContainer<?> javaContainer = createJavaContainer("eclipse-temurin:17", tempDir)) {
            javaContainer.start();
            ExecResult execResult = javaContainer.execInContainer("sh", "-c",
                    String.format("cd %s/repo; java -classpath '*' org.ohdsi.databases.DBConnector", APPDIR_IN_CONTAINER));
            if (execResult.getExitCode() != 0) {
                System.out.println("stdout:" + execResult.getStdout());
                System.out.println("stderr:" + execResult.getStderr());
            }
            assertTrue(execResult.getStdout().contains(DBConnector.ALL_JDBC_DRIVERS_LOADABLE), "Not all supported JDBC drivers could be loaded");
            javaContainer.execInContainer("sh", "-c", "rm /app/repo/snowflake*"); // sabotage, confirms that test breaks if driver missing
            execResult = javaContainer.execInContainer("sh", "-c",
                    String.format("cd %s/repo; java -classpath '*' org.ohdsi.databases.DBConnector", APPDIR_IN_CONTAINER));
            assertFalse(execResult.getStdout().contains(DBConnector.ALL_JDBC_DRIVERS_LOADABLE), "Not all supported JDBC drivers could be loaded");
        }
    }

    @Test
    void verifySnowflakeFailureInJava17() throws IOException, URISyntaxException, InterruptedException {
        // only run this when a snowflake.env file is available
        Assumptions.assumeTrue(new ScanTestUtils.PropertiesFileChecker("snowflake.env"), "Snowflake system properties file not available");
        /*
         * There is an issue with Snowflake JDBC that causes a failure in Java 16 and later
         * (see https://community.snowflake.com/s/article/JDBC-Driver-Compatibility-Issue-With-JDK-16-and-Later)
         * A flag can be passed to the JVM to work around this: --add-opens=java.base/java.nio=ALL-UNNAMED
         *
         * The whiteRabbit script in the distribution passes this flag.
         *
         * The tests below verify that:
         * - the flag does not cause problems when running with Java 8 (1.8) or 11
         * - without the flag, a failure occurs when running with Java 17
         * - passing the flag fixes the failure with Java 17
         *
         * As the flag is in the distributed script, it needs to be edited out of the script.
         *
         * Note that we only test with the LTS versions of Java. This leaves Java 16 untested and unfixed.
         *
         * Once a fix is available in a newer version of the Snowflake JDBC jar, and it is used in WhiteRabbit,
         * The test that now confirms the issue by expecting an Assertion error should start to fail.
         * Then it is time to remove the flag (it is in the pom.xml for the whiterabbit module), and remove these tests,
         * or normalize them to simply verify that all works well.
         */
        String patchingFlag = "--add-opens=java.base/java.nio=ALL-UNNAMED";
        String javaOpts = String.format("JAVA_OPTS='%s'", patchingFlag);

        runDistributionWithSnowflake("eclipse-temurin:8", "");

        AssertionError ignoredError = Assertions.assertThrows(org.opentest4j.AssertionFailedError.class, () -> {
            runDistributionWithSnowflake("eclipse-temurin:8", javaOpts);
        });

        // verify that the flag as set in the whiteRabbit script does not have an adversary effect when running with Java 11
        // note that this flag is not supported by Java 8 (1.8)
        runDistributionWithSnowflake("eclipse-temurin:11", javaOpts);

        // verify that the failure occurs when running with Java 17, without the flag
        ignoredError = Assertions.assertThrows(org.opentest4j.AssertionFailedError.class, () -> {
            runDistributionWithSnowflake("eclipse-temurin:17","");
        });

        // finally, verify that passing the flag fixes the failure when running wuth Java 17
        runDistributionWithSnowflake("eclipse-temurin:17", javaOpts);
    }

    void runDistributionWithSnowflake(String javaImageName, String javaOpts) throws IOException, InterruptedException, URISyntaxException {
        // test only run when there are settings available for Snowflake; otherwise it should be skipped
        Assumptions.assumeTrue(new ScanTestUtils.PropertiesFileChecker("snowflake.env"), "Snowflake system properties file not available");
        ScanTestUtils.PropertyReader reader = new ScanTestUtils.PropertyReader();
        try (GenericContainer<?> testContainer = createPythonContainer()) {
            prepareSnowflakeTestData(testContainer, reader);
            testContainer.stop();

            try (GenericContainer<?> javaContainer = createJavaContainer(javaImageName, tempDir)) {
                javaContainer.start();
                Charset charset = StandardCharsets.UTF_8;
                Path iniFile = tempDir.resolve("snowflake.ini");
                URL iniTemplate = VerifyDistributionIT.class.getClassLoader().getResource("scan_data/snowflake.ini.template");
                URL referenceScanReport = SourceDataScanSnowflakeIT.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sql.xlsx");
                assert iniTemplate != null;
                String content = new String(Files.readAllBytes(Paths.get(iniTemplate.toURI())), charset);
                content = content.replace("%WORKING_FOLDER%", WORKDIR_IN_CONTAINER)
                        .replace("%SNOWFLAKE_ACCOUNT%", reader.getOrFail("SNOWFLAKE_WR_TEST_ACCOUNT"))
                        .replace("%SNOWFLAKE_USER%", reader.getOrFail("SNOWFLAKE_WR_TEST_USER"))
                        .replace("%SNOWFLAKE_PASSWORD%", reader.getOrFail("SNOWFLAKE_WR_TEST_PASSWORD"))
                        .replace("%SNOWFLAKE_WAREHOUSE%", reader.getOrFail("SNOWFLAKE_WR_TEST_WAREHOUSE"))
                        .replace("%SNOWFLAKE_DATABASE%", reader.getOrFail("SNOWFLAKE_WR_TEST_DATABASE"))
                        .replace("%SNOWFLAKE_SCHEMA%", reader.getOrFail("SNOWFLAKE_WR_TEST_SCHEMA"));
                Files.write(iniFile, content.getBytes(charset));
                // verify that the distribution of whiterabbit has been generated and is available inside the container
                ExecResult execResult = javaContainer.execInContainer("sh", "-c", String.format("ls %s", APPDIR_IN_CONTAINER));
                assertTrue(execResult.getStdout().contains("repo"), "WhiteRabbit distribution is not accessible inside container");

                javaContainer.copyFileToContainer(MountableFile.forHostPath(tempDir), WORKDIR_IN_CONTAINER);

                // run whiterabbit and verify the result
                execResult = javaContainer.execInContainer("sh", "-c",
                        String.format("%s /app/bin/whiteRabbit -ini %s/snowflake.ini", javaOpts, WORKDIR_IN_CONTAINER));
                verifyResultOrLog(execResult, "Started new scan of 2 tables...");
                verifyResultOrLog(execResult, "Scanning table PERSON");
                verifyResultOrLog(execResult, "Scanning table COST");
                verifyResultOrLog(execResult, "Scan report generated: /whiterabbit/ScanReport.xlsx");

                javaContainer.copyFileFromContainer("/whiterabbit/ScanReport.xlsx", tempDir.resolve("ScanReport.xlsx").toString());

                assertTrue(ScanTestUtils.scanResultsSheetMatchesReference(tempDir.resolve("ScanReport.xlsx"), Paths.get(referenceScanReport.toURI()), DbType.SNOWFLAKE));
            }
        }
    }

    private void verifyResultOrLog(ExecResult execResult, String expected) {
        if (execResult.getExitCode() != 0) {
            System.out.println("stdout:" + execResult.getStdout());
            System.out.println("stderr:" + execResult.getStderr());
        }
        assertEquals(0, execResult.getExitCode());
        assertTrue(execResult.getStdout().contains(expected));
    }

    // intent of this test: verify that downloading and unzipping the BigQuery JDBC dependencies
    // works as intended.
    // The license of the BigQuery JDBC jar does not allow distribution; users who wish to use
    // bigquery have to download and install the dependencies themselves.
    // This test verifies that an attempt to run WhiteRabbit for BigQuery fails if the jar is
    // not installed, then install it, and repeats the test, expecting success.
    //
    // PLEASE NOTE that this test can only run if a valid json file with application default credentials for gcloud
    // is provided through a file `bigquery.env` in the root of the project.
    @Test
    void runDistributionWithBigQuery() throws IOException, InterruptedException, URISyntaxException {
        // test only run when there are settings available for BigQuery; otherwise it should be skipped
        String bigQueryDriverUrl = "https://storage.googleapis.com/simba-bq-release/jdbc/SimbaJDBCDriverforGoogleBigQuery42_1.5.2.1005.zip";
        Assumptions.assumeTrue(new ScanTestUtils.PropertiesFileChecker("bigquery.env"), "BigQuery system properties file not available");
        ScanTestUtils.PropertyReader reader = new ScanTestUtils.PropertyReader();
        try (GenericContainer<?> testContainer = createPythonContainer()) {
            testContainer.stop();

            try (GenericContainer<?> javaContainer = createJavaContainer("eclipse-temurin:17", tempDir)) {
                javaContainer.start();
                Charset charset = StandardCharsets.UTF_8;
                Path iniFile = tempDir.resolve("bigquery.ini");
                URL iniTemplate = VerifyDistributionIT.class.getClassLoader().getResource("scan_data/bigquery.ini.template");
                URL referenceScanReport = SourceDataScanSnowflakeIT.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sql.xlsx");
                assert iniTemplate != null;
                String content = new String(Files.readAllBytes(Paths.get(iniTemplate.toURI())), charset);
                content = content.replace("%WORKING_FOLDER%", WORKDIR_IN_CONTAINER)
                        .replace("%SERVER_LOCATION%", reader.getOrFail("BIGQUERY_PROJECT_ID"))
                        .replace("%USER_NAME%", "")
                        .replace("%PASSWORD%", "/whiterabbit/application_default_credentials.json")
                        .replace("%DATABASE_NAME%", reader.getOrFail("BIGQUERY_DATASET"));
                Files.write(iniFile, content.getBytes(charset));
                Files.copy(Paths.get(reader.getOrFail("BIGQUERY_KEY_FILE")), Paths.get(tempDir.toString(), "application_default_credentials.json"));
                javaContainer.copyFileToContainer(MountableFile.forHostPath(tempDir), WORKDIR_IN_CONTAINER);

                // verify that the distribution of whiterabbit has been generated and is available inside the container
                ExecResult execResult = javaContainer.execInContainer("sh", "-c", String.format("ls %s", APPDIR_IN_CONTAINER));
                assertTrue(execResult.getStdout().contains("repo"), "WhiteRabbit distribution is not accessible inside container");

                // make the gcloud credentials available in the right location
                execResult = javaContainer.execInContainer("sh", "-c",
                        "mkdir -p /root/.config/gcloud; cp /whiterabbit/application_default_credentials.json /root/.config/gcloud/;");

                // run whiteRabbit and verify failure because the BigQuery JDBC jar is not present
                execResult = javaContainer.execInContainer("sh", "-c",
                        String.format("/app/bin/whiteRabbit -ini %s/bigquery.ini", WORKDIR_IN_CONTAINER));
                assertNotEquals(0, execResult.getExitCode());
                assertTrue(execResult.getStderr().contains("java.lang.RuntimeException: Cannot find Simba GoogleBigQuery JDBC Driver class"));

                // install the BigQuery JDBC jar, run whiteRabbit and verify success
                installBigQueryJDBCJar(bigQueryDriverUrl, javaContainer);
                execResult = javaContainer.execInContainer("sh", "-c",
                        String.format("/app/bin/whiteRabbit -ini %s/bigquery.ini", WORKDIR_IN_CONTAINER));
                assertEquals(0, execResult.getExitCode());

                // For easier setup, do not verify results, not having a failure for the missing BigQuery JDBC jar is sufficient.
                assertTrue(execResult.getStdout().contains("Started new scan of 0 tables..."));
                assertTrue(execResult.getStdout().contains("Scan report generated: /whiterabbit/ScanReport.xlsx"));
            }
        }
    }

    private void installBigQueryJDBCJar(String bigQueryDriverUrl, GenericContainer<?> javaContainer) throws IOException, InterruptedException {
        if (StringUtils.isNotEmpty(bigQueryDriverUrl)) {
            String[] parts = bigQueryDriverUrl.split("/");
            String command =
                    "apt -qq -y update; apt -qq -y install unzip; " +
                            "cd /app/repo; " +
                            "wget " + bigQueryDriverUrl + "; " +
                            "unzip -qq -o " + parts[parts.length - 1] + ";";
            ExecResult execResult = javaContainer.execInContainer("sh", "-c", command);
            if (execResult.getExitCode() != 0) {
                System.out.println("stdout: " + execResult.getStdout());
                System.out.println("stderr: " + execResult.getStderr());
            }
        }
    }

    private void testWhiteRabbitInContainer(String imageName, String expectedVersion) throws IOException, InterruptedException, URISyntaxException {
        try (GenericContainer<?> javaContainer = createJavaContainer(imageName, tempDir)) {
            javaContainer.start();

            Charset charset = StandardCharsets.UTF_8;
            Path iniFile = tempDir.resolve("tsv.ini");
            URL iniTemplate = VerifyDistributionIT.class.getClassLoader().getResource("scan_data/tsv.ini.template");
            URL referenceScanReport = VerifyDistributionIT.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-csv.xlsx");
            Path personCsv = Paths.get(VerifyDistributionIT.class.getClassLoader().getResource("scan_data/person-header.csv").toURI());
            Path costCsv = Paths.get(VerifyDistributionIT.class.getClassLoader().getResource("scan_data/cost-header.csv").toURI());
            assertNotNull(iniTemplate);
            String content = new String(Files.readAllBytes(Paths.get(iniTemplate.toURI())), charset);
            content = content.replaceAll("%WORKING_FOLDER%", WORKDIR_IN_CONTAINER);
            Files.write(iniFile, content.getBytes(charset));
            Files.copy(personCsv, tempDir.resolve("person.csv"), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(costCsv, tempDir.resolve("cost.csv"), StandardCopyOption.REPLACE_EXISTING);

            // verify that the default java version in the container is actually 1.8
            ExecResult execResult = javaContainer.execInContainer("sh", "-c", "java -version");
            assertTrue(execResult.getStderr().startsWith(expectedVersion), "default java version in container should match version " + expectedVersion);

            javaContainer.copyFileToContainer(MountableFile.forHostPath(tempDir), WORKDIR_IN_CONTAINER);
            // verify that the distribution of whiterabbit has been generated and is available inside the container
            execResult = javaContainer.execInContainer("sh", "-c", String.format("ls %s", APPDIR_IN_CONTAINER));
            assertTrue(execResult.getStdout().contains("repo"), "WhiteRabbit distribution is not accessible inside container");

            // run whiterabbit and verify the result
            execResult = javaContainer.execInContainer("sh", "-c", String.format("/app/bin/whiteRabbit -ini %s/tsv.ini", WORKDIR_IN_CONTAINER));
            if (execResult.getExitCode() != 0) {
                logger.error("stdout:" + execResult.getStdout());
                logger.error("stderr:" + execResult.getStderr());
            }
            assertTrue(execResult.getStdout().contains("Started new scan of 2 tables..."));
            assertTrue(execResult.getStdout().contains("Scanning table /whiterabbit/person.csv"));
            assertTrue(execResult.getStdout().contains("Scanning table /whiterabbit/cost.csv"));
            assertTrue(execResult.getStdout().contains("Scan report generated: /whiterabbit/ScanReport.xlsx"));

            javaContainer.copyFileFromContainer("/whiterabbit/ScanReport.xlsx", tempDir.resolve("ScanReport.xlsx").toString());

            assertTrue(ScanTestUtils.scanResultsSheetMatchesReference(tempDir.resolve("ScanReport.xlsx"), Paths.get(referenceScanReport.toURI()), DbType.DELIMITED_TEXT_FILES));

            javaContainer.stop();
        }
    }

    private GenericContainer<?> createJavaContainer(String imageName, Path workingDir) {
        return new GenericContainer<>(
                DockerImageName.parse(imageName))
                .withCommand("sh", "-c", "tail -f /dev/null")
                .withCopyToContainer(
                        MountableFile.forHostPath("../dist/"),
                        APPDIR_IN_CONTAINER)
                .withCopyToContainer(
                        MountableFile.forHostPath(workingDir),
                        WORKDIR_IN_CONTAINER);
    }
}
