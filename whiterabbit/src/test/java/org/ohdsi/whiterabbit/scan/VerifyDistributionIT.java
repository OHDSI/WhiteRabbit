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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.runners.Parameterized;
import org.ohdsi.databases.DBConnector;
import org.ohdsi.databases.SnowflakeTestUtils;
import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.utilities.files.IniFile;
import org.ohdsi.whiterabbit.WhiteRabbitMain;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.ohdsi.whiterabbit.scan.SourceDataScanSnowflakeIT.*;

/**
 * Intent: "deploy" the distributed application in a docker container (TestContainer) containing a Java runtime
 * of a specified version, and runs a test of WhiteRabbit that aim to verify that the distribution is complete,
 * i.e. no dependencies are missing. A data for a scan on csv files is used to run whiterabbit.
 *
 * Note that this does not test any of the JDBC driver dependencies, unless these databases are actually used.
 */
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
        try (GenericContainer<?> javaContainer = createJavaContainer("eclipse-temurin:11")) {
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

    //@Test // useful while developing/debugging, leaving in place to test again after Snowflake JDBC driver update
    void verifySnowflakeFailureInJava17() throws IOException, URISyntaxException, InterruptedException {
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

        // verify that the flag as set in the whiteRabbit script does not have an adversary effect when running with Java 11
        // note that this flag is not supported by Java 8 (1.8)
        runDistributionWithSnowflake("eclipse-temurin:11",javaOpts);

        // verify that the failure occurs when running with Java 17, without the flag
        AssertionError ignoredError = Assertions.assertThrows(org.opentest4j.AssertionFailedError.class, () -> {
            runDistributionWithSnowflake("eclipse-temurin:17","");
        });

        // finally, verify that passing the flag fixes the failure when running wuth Java 17
        runDistributionWithSnowflake("eclipse-temurin:17",javaOpts);
    }

    void runDistributionWithSnowflake(String javaImageName, String javaOpts) throws IOException, InterruptedException, URISyntaxException {
        // test only run when there are settings available for Snowflake; otherwise it should be skipped
        Assumptions.assumeTrue(new SnowflakeTestUtils.SnowflakeSystemPropertiesFileChecker(), "Snowflake system properties file not available");
        SnowflakeTestUtils.PropertyReader reader = new SnowflakeTestUtils.PropertyReader();
        try (GenericContainer<?> testContainer = createPythonContainer()) {
            prepareTestData(testContainer, reader);
            testContainer.stop();

            try (GenericContainer<?> javaContainer = createJavaContainer(javaImageName)) {
                javaContainer.start();
                Charset charset = StandardCharsets.UTF_8;
                Path iniFile = tempDir.resolve("snowflake.ini");
                URL iniTemplate = VerifyDistributionIT.class.getClassLoader().getResource("scan_data/snowflake.ini.template");
                URL referenceScanReport = SourceDataScanSnowflakeIT.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sql.xlsx");
                assert iniTemplate != null;
                String content = new String(Files.readAllBytes(Paths.get(iniTemplate.toURI())), charset);
                content = content.replaceAll("%WORKING_FOLDER%", WORKDIR_IN_CONTAINER)
                        .replaceAll("%SNOWFLAKE_ACCOUNT%", reader.getOrFail("SNOWFLAKE_WR_TEST_ACCOUNT"))
                        .replaceAll("%SNOWFLAKE_USER%", reader.getOrFail("SNOWFLAKE_WR_TEST_USER"))
                        .replaceAll("%SNOWFLAKE_PASSWORD%", reader.getOrFail("SNOWFLAKE_WR_TEST_PASSWORD"))
                        .replaceAll("%SNOWFLAKE_WAREHOUSE%", reader.getOrFail("SNOWFLAKE_WR_TEST_WAREHOUSE"))
                        .replaceAll("%SNOWFLAKE_DATABASE%", reader.getOrFail("SNOWFLAKE_WR_TEST_DATABASE"))
                        .replaceAll("%SNOWFLAKE_SCHEMA%", reader.getOrFail("SNOWFLAKE_WR_TEST_SCHEMA"));
                Files.write(iniFile, content.getBytes(charset));
                // verify that the distribution of whiterabbit has been generated and is available inside the container
                ExecResult execResult = javaContainer.execInContainer("sh", "-c", String.format("ls %s", APPDIR_IN_CONTAINER));
                assertTrue(execResult.getStdout().contains("repo"), "WhiteRabbit distribution is not accessible inside container");

                // run whiterabbit and verify the result
                execResult = javaContainer.execInContainer("sh", "-c",
                        String.format("%s /app/bin/whiteRabbit -ini %s/snowflake.ini", javaOpts, WORKDIR_IN_CONTAINER));
                assertTrue(execResult.getStdout().contains("Started new scan of 2 tables..."));
                assertTrue(execResult.getStdout().contains("Scanning table PERSON"));
                assertTrue(execResult.getStdout().contains("Scanning table COST"));
                assertTrue(execResult.getStdout().contains("Scan report generated: /whiterabbit/ScanReport.xlsx"));

                assertTrue(ScanTestUtils.scanResultsSheetMatchesReference(tempDir.resolve("ScanReport.xlsx"), Paths.get(referenceScanReport.toURI()), DbType.SNOWFLAKE));
            }
        }
    }

    private void testWhiteRabbitInContainer(String imageName, String expectedVersion) throws IOException, InterruptedException, URISyntaxException {
        try (GenericContainer<?> javaContainer = createJavaContainer(imageName)) {
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

            assertTrue(ScanTestUtils.scanResultsSheetMatchesReference(tempDir.resolve("ScanReport.xlsx"), Paths.get(referenceScanReport.toURI()), DbType.DELIMITED_TEXT_FILES));

            javaContainer.stop();
        }
    }

    private GenericContainer<?> createJavaContainer(String imageName) {
        return new GenericContainer<>(
                DockerImageName.parse(imageName))
                .withCommand("sh", "-c", "tail -f /dev/null")
                .withFileSystemBind(Paths.get("../dist").toAbsolutePath().toString(), APPDIR_IN_CONTAINER)
                .withFileSystemBind(tempDir.toString(), WORKDIR_IN_CONTAINER, BindMode.READ_WRITE);
    }
}
