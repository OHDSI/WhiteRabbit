package org.ohdsi.whiterabbit.scan;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


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
    void testProcess(@TempDir Path tempDir) {
        Path outFile = tempDir.resolve("scanresult.xslx");
        SourceDataScan sourceDataScan = new SourceDataScan();
        DbSettings dbSettings = getTestDbSettings();

        sourceDataScan.process(dbSettings, outFile.toString());
        assertTrue(Files.exists(outFile));
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
}