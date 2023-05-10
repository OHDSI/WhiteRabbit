package org.ohdsi.whiterabbit.scan;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.ooxml.ReadXlsxFileWithHeader;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.RowUtilities;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;


@Testcontainers
@Tag("DockerRequired")
class TestSourceDataScan {

    @Container
    public static PostgreSQLContainer<?> postgreSQL;

    static {
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
        try {
            postgreSQL = new PostgreSQLContainer<>("postgres:13.1")
                    .withUsername("test")
                    .withPassword("test")
                    .withDatabaseName("test")
                    .withClasspathResourceMapping(
                            "scan_data",
                            "/scan_data",
                            BindMode.READ_ONLY)
                    .withInitScript("scan_data/create_data_postgresql.sql");

            postgreSQL.start();

        } finally {
            if (postgreSQL != null) {
                postgreSQL.stop();
            }
        }
    }

    void testProcess(Path tempDir) throws IOException {
        Path outFile = tempDir.resolve(SourceDataScan.SCAN_REPORT_FILE_NAME);
        SourceDataScan sourceDataScan = new SourceDataScan();
        DbSettings dbSettings = ScanTestUtils.getTestPostgreSQLSettings(postgreSQL);

        sourceDataScan.process(dbSettings, outFile.toString());
        ScanTestUtils.verifyScanResultsFromXSLX(outFile, dbSettings.dbType);
    }

    @Test
    void testApachePoiTmpFileProblemWithAutomaticResolution(@TempDir Path tempDir) throws IOException, ReflectiveOperationException {
        // intends to verify solution of this bug: https://github.com/OHDSI/WhiteRabbit/issues/293

        /*
         * This tests a fix that assumes that the bug referenced here occurs in a multi-user situation where the
         * first user running the scan, and causing /tmp/poifiles to created, does so by creating it read-only
         * for everyone else. This directory is not automatically cleaned up, so every following user on the same
         * system running the scan encounters the problem that /tmp/poifiles already exists and is read-only,
         * causing a crash when the Apacho poi library attemps to create the xslx file.
         *
         * The class SourceDataScan has been extended with a static method, called implicitly once through a static{}
         * block, to create a TempDir strategy that will create a unique directory for each instance/run of WhiteRabbit.
         * This effectively solves the assumed error situation.
         *
         * This test does not execute a multi-user situation, but emulates it by leaving the tmp directory in a
         * read-only state after the first scan, and then confirming that a second scan fails. After that,
         * a new unique tmp dir is enforced by invoking SourceDataScan.setUniqueTempDirStrategyForApachePoi(),
         * and a new scan now runs successfully.
         */

        // Make sure the scenarios are tested without a user configured tmp dir, so set environment variable and
        // system property to an empty value
        System.setProperty(SourceDataScan.POI_TMP_DIR_PROPERTY_NAME, "");
        updateEnv(SourceDataScan.POI_TMP_DIR_ENVIRONMENT_VARIABLE_NAME, "");
        Path defaultTmpPath = SourceDataScan.getDefaultPoiTmpPath(tempDir);

        if (!Files.exists(defaultTmpPath)) {
            Files.createDirectory(defaultTmpPath);
        } else {
            if (Files.exists(defaultTmpPath.resolve(SourceDataScan.SCAN_REPORT_FILE_NAME))) {
                Files.delete(defaultTmpPath.resolve(SourceDataScan.SCAN_REPORT_FILE_NAME));
            }
        }

        // process should pass without problem, and afterwards the default tmp dir should exist
        testProcess(defaultTmpPath);
        assertTrue(Files.exists(defaultTmpPath));

        // provoke the problem situation. make the default tmp dir readonly, try to process again
        assertTrue(Files.deleteIfExists(defaultTmpPath.resolve(SourceDataScan.SCAN_REPORT_FILE_NAME))); // or Apache Poi will happily reuse it
        assertTrue(defaultTmpPath.toFile().setReadOnly());
        System.out.println("defaultTmpPath: " + defaultTmpPath.toFile().getAbsolutePath());
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
                    testProcess(defaultTmpPath);
            System.out.println("Hold it!");
                });
        assertTrue(thrown.getMessage().contains("Permission denied"));

        // invoke the static method to set a new tmp dir, process again (should succeed) and verify that
        // the new tmpdir is indeed different from the default
        String myTmpDir = SourceDataScan.setUniqueTempDirStrategyForApachePoi();
        testProcess(Paths.get(myTmpDir));
        assertNotEquals(defaultTmpPath.toFile().getAbsolutePath(), myTmpDir);

        // we might have left behind an unworkable situation; attempt to solve that
        if (Files.exists(defaultTmpPath) && !Files.isWritable(defaultTmpPath)) {
            assertTrue(defaultTmpPath.toFile().setWritable(true));
        }
    }

    @Test
    void testApachePoiTmpFileProblemWithUserConfiguredResolution(@TempDir Path tempDir) throws IOException, ReflectiveOperationException {
        // 1. Verify that the poi tmp dir property is used, if set
        Path tmpDirFromProperty = tempDir.resolve("setByProperty");
        System.setProperty(SourceDataScan.POI_TMP_DIR_PROPERTY_NAME, tmpDirFromProperty.toFile().getAbsolutePath());
        Files.createDirectories(tmpDirFromProperty);

        SourceDataScan.setUniqueTempDirStrategyForApachePoi(); // need to reset to pick up the property
        testProcess(tmpDirFromProperty);
        assertTrue(Files.exists(tmpDirFromProperty));

        cleanTmpDir(tmpDirFromProperty);

        // 2. Verify that the poi tmp dir environment variable is used, if set, and overrules the property set above
        Path tmpDirFromEnvironmentVariable = tempDir.resolve("setByEnvVar");
        updateEnv(SourceDataScan.POI_TMP_DIR_ENVIRONMENT_VARIABLE_NAME, tmpDirFromEnvironmentVariable.toFile().getAbsolutePath());
        Files.createDirectories(tmpDirFromEnvironmentVariable);

        SourceDataScan.setUniqueTempDirStrategyForApachePoi(); // need to reset to pick up the env. var.
        testProcess(tmpDirFromEnvironmentVariable);
        assertFalse(Files.exists(tmpDirFromProperty));
        assertTrue(Files.exists(tmpDirFromEnvironmentVariable));
        cleanTmpDir(tmpDirFromEnvironmentVariable);
    }

    @SuppressWarnings({ "unchecked" })
    private static void updateEnv(String name, String val) throws ReflectiveOperationException {
        Map<String, String> env = System.getenv();
        Field field = env.getClass().getDeclaredField("m");
        field.setAccessible(true);
        ((Map<String, String>) field.get(env)).put(name, val);
    }
    private List<String> getTableNames(DbSettings dbSettings) {
        try (RichConnection richConnection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType)) {
            return richConnection.getTableNames("public");
        }
    }

    private static void cleanTmpDir(Path path) {
        if (Files.exists(path)) {
            if (!Files.isWritable(path)) {
                assertTrue(path.toFile().setWritable(true),
                        String.format("This test cannot run properly if %s exists but is not writeable. Either remove it or make it writeable",
                                path.toFile().getAbsolutePath()));
            }
            assertTrue(deleteDir(path.toFile()));
        }
    }
    private static boolean deleteDir(File file) {
        if (Files.exists(file.toPath())) {
            File[] contents = file.listFiles();
            if (contents != null) {
                for (File f : contents) {
                    if (!Files.isSymbolicLink(f.toPath())) {
                        deleteDir(f);
                    }
                }
            }
            return file.delete();
        }
        return true;
    }
}
