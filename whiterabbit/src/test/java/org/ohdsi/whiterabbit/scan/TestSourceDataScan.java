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
                            "postgresql_data",
                            "/postgresql_data",
                            BindMode.READ_ONLY)
                    .withInitScript("postgresql_data/create_synpuf1k.sql");

            postgreSQL.start();

        } finally {
            if (postgreSQL != null) {
                postgreSQL.stop();
            }
        }
    }

    @Test
    public void connectToDatabase() {
        // this is also implicitly tested by tesProcess(), but having it fail separately helps identify problems quicker
        DbSettings dbSettings = getTestDbSettings();
        try (RichConnection richConnection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType)) {
            // do nothing, connection will be closed automatically because RichConnection implements interface Closeable
        }
    }

    @Test
    public void testGetTableNames() {
        // this is also implicitly tested by tesProcess(), but having it fail separately helps identify problems quicker
        DbSettings dbSettings = getTestDbSettings();
        List<String> tableNames = getTableNames(dbSettings);
        assertEquals(18, tableNames.size());
    }
    @Test
    void testProcess(@TempDir Path tempDir) throws IOException {
        Path outFile = tempDir.resolve("scanresult.xslx");
        SourceDataScan sourceDataScan = new SourceDataScan();
        DbSettings dbSettings = getTestDbSettings();

        sourceDataScan.process(dbSettings, outFile.toString());
        assertTrue(Files.exists(outFile));

	    //
        // Verify the contents of the generated xlsx file
	    //
        FileInputStream file = new FileInputStream(new File(outFile.toUri()));

        ReadXlsxFileWithHeader sheet = new ReadXlsxFileWithHeader(file);

        List<Row> data = new ArrayList<>();
        int i = 0;
        for (Row row : sheet) {
            data.add(row);
            i++;
        }

        // apparently the order of rows in the generated xslx table is not fixed,
        // so they need to be sorted to be able to verify their contents
        RowUtilities.sort(data, "Table", "Field");
        assertEquals(243, i);

        // since the table is generated with empty lines between the different tables of the source database,
        // a number of empty lines is expected. Verify this, and the first non-empty line
        expectRowNIsLike(0, data, "", "", "", "", "", "");
        expectRowNIsLike(17, data, "", "", "", "", "", "");
        expectRowNIsLike(18, data, "care_site", "care_site_id", "", "integer", "0", "23259");

        // sample some other rows in the available range
        expectRowNIsLike(33, data,"cdm_source", "vocabulary_version", "", "character varying", "0", "1");
        expectRowNIsLike(34, data,"condition_era", "condition_concept_id", "", "integer", "0", "99855");
        expectRowNIsLike(66, data,"cost", "paid_dispensing_fee", "", "numeric", "0", "367378");
        expectRowNIsLike(155, data,"observation", "observation_datetime", "", "timestamp without time zone", "0", "19339");
        expectRowNIsLike(242, data, "visit_occurrence", "visit_type_concept_id", "", "integer", "0", "55261");
    }

    @Test
    void testApachePoiTmpfileProblem(@TempDir Path tempDir) throws IOException {
        // intends to verify solution of this bug: https://github.com/OHDSI/WhiteRabbit/issues/293

        /*
         * This tests a fix that assumes that the bug referenced here occurs in a multi-user situation where the
         * first user running the scan, and causing /tmp/poifiles to created, does so by creating it read-only
         * for everyone else. This directory is not automatically cleaned up, so every following user on the same
         * system running the scan encounters the problem that /tmp/poifiles already exists and is read-only,
         * causing a crash when the Apacho poi library attemps to create the xslx file.
         *
         * The class SourceDataScan has been extended with a static method, called implicitly once through a static{}
         * block, to create a TempDir strategy to create a
         * unique directory for each instance/run of WhiteRabbit. This effectively solves the assumes error
         * situation.
         *
         * This test does not execute a multi-user situation, but emulates it by leaving the tmp directory in a
         * read-only state after the first scan, and then confirming that a second scan fails. After that,
         * a new unique tmp dir is enforced by invoking SourceDataScan.setUniqueTempDirStrategyForApachePoi(),
         * and a new scan now runs successfully.
         */
        // TODO: if/when updating poi to 5.x or higher, use DefaultTempFileCreationStrategy.POIFILES instead of a string literal
        final String poiFilesDir = "poifiles";
        Path defaultTmpPath = Paths.get(FileUtils.getTempDirectory().getAbsolutePath(), poiFilesDir);

        // attempt to resolve an already existing unworkable situation where the default tmp dir for poi files is readonly
        if (Files.exists(defaultTmpPath) && !Files.isWritable(defaultTmpPath)) {
            assertTrue(defaultTmpPath.toFile().setWritable(true),
                    String.format("This test cannot run properly if %s exists but is not writeable. Either remove it or make it writeable",
                            defaultTmpPath.toFile().getAbsolutePath()));
        }

        // process should pass without problem, and afterwards the default tmp dir should exist
        testProcess(tempDir);
        assertTrue(Files.exists(defaultTmpPath));

        // provoke the problem situation. make the default tmp dir readonly, try to process again
        assertTrue(defaultTmpPath.toFile().setReadOnly());
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
                    testProcess(tempDir);
                });
        assertTrue(thrown.getMessage().endsWith("Permission denied"));

        // invoke the static method to set a new tmp dir, process again (should succeed) and verify that
        // the new tmpdir is indeed different from the default
        String myTmpDir = SourceDataScan.setUniqueTempDirStrategyForApachePoi();
        testProcess(tempDir);
        assertNotEquals(defaultTmpPath.toFile().getAbsolutePath(), myTmpDir);

        // we might have left behind an unworkable situation; attempt to solve that
        if (Files.exists(defaultTmpPath) && !Files.isWritable(defaultTmpPath)) {
            assertTrue(defaultTmpPath.toFile().setWritable(true));
        }

    }

    private boolean expectRowNIsLike(int n, List<Row> rows, String... expectedValues) {
        assert expectedValues.length == 6;
        testColumnValue(n, rows.get(n), "Table", expectedValues[0]);
        testColumnValue(n, rows.get(n), "Field", expectedValues[1]);
        testColumnValue(n, rows.get(n), "Description", expectedValues[2]);
        testColumnValue(n, rows.get(n), "Type", expectedValues[3]);
        testColumnValue(n, rows.get(n), "Max length", expectedValues[4]);
        testColumnValue(n, rows.get(n), "N rows", expectedValues[5]);
        return true;
    }

    private void testColumnValue(int i, Row row, String fieldName, String expected) {
        if (!expected.equals(row.get(fieldName))) {
            fail(String.format("In row %d, value '%s' was expected for column '%s', but '%s' was found",
                    i, expected, fieldName, row.get(fieldName)));
        }
    }
    private List<String> getTableNames(DbSettings dbSettings) {
        try (RichConnection richConnection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType)) {
            return richConnection.getTableNames("public");
        }
    }

    private DbSettings getTestDbSettings() {
        DbSettings dbSettings = new DbSettings();
        dbSettings.dbType = DbType.POSTGRESQL;
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;
        dbSettings.server = postgreSQL.getJdbcUrl();
        dbSettings.database = "public"; // yes, really
        dbSettings.user = postgreSQL.getUsername();
        dbSettings.password = postgreSQL.getPassword();
        dbSettings.tables = getTableNames(dbSettings);

        return dbSettings;
    }

    private boolean deleteDir(File file) {
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
