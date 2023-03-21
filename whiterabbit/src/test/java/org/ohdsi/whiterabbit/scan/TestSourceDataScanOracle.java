package org.ohdsi.whiterabbit.scan;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;


@Testcontainers(disabledWithoutDocker = true)
class TestSourceDataScanOracle {

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

    @BeforeEach
    public void disableOracleregionTimezoneCheck() {
        System.out.println("Setting -Doracle.jdbc.timezoneAsRegion=false");
        System.setProperty("oracle.jdbc.timezoneAsRegion", "false");
    }
    @Test
    public void connectToDatabase() {
        // this is also implicitly tested by testSourceDataScan(), but having it fail separately helps identify problems quicker
        DbSettings dbSettings = getTestDbSettings();
        try (RichConnection richConnection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType)) {
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
    void testSourceDataScan(@TempDir Path tempDir) throws IOException {
        loadData();
        Path outFile = tempDir.resolve("scanresult.xslx");
        SourceDataScan sourceDataScan = new SourceDataScan();
        DbSettings dbSettings = getTestDbSettings();

        sourceDataScan.process(dbSettings, outFile.toString());
        ScanTestUtils.verifyScanResultsFromXSLX(outFile, dbSettings.dbType);
    }

    private void loadData() {
        insertDataFromCsv("person");
        insertDataFromCsv("cost");
    }

    private void insertDataFromCsv(String tableName) {
        DbSettings dbSettings = getTestDbSettings();
        try (RichConnection richConnection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType)) {
            try (BufferedReader reader = new BufferedReader(getResourcePath(tableName))) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    String[] values = line.split("\t");
                    if (line.endsWith("\t")) {
                        values = Arrays.copyOf(values, values.length + 1);
                        values[values.length - 1] = "";
                    }
                    String insertSql = String.format("INSERT INTO %s.%s VALUES('%s');", dbSettings.database, tableName, String.join("','", values));
                    richConnection.execute(insertSql);
                }
            } catch (IOException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private InputStreamReader getResourcePath(String tableName) throws URISyntaxException, IOException {
        String resourceName = String.format("scan_data/%s.csv", tableName);

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).toURI());
        return new InputStreamReader(Files.newInputStream(file.toPath()));
    }

    private List<String> getTableNames(DbSettings dbSettings) {
        try (RichConnection richConnection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType)) {
            return richConnection.getTableNames(SCHEMA_NAME);
        }
    }

    private DbSettings getTestDbSettings() {
        DbSettings dbSettings = new DbSettings();
        dbSettings.dbType = DbType.ORACLE;
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;
        //dbSettings.server = oracleContainer.getJdbcUrl();
        dbSettings.server = String.format("%s:%s/%s", "localhost", oracleContainer.getOraclePort(), "XE");
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
