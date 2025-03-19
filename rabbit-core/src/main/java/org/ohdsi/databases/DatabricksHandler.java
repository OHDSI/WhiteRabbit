package org.ohdsi.databases;

import org.ohdsi.databases.configuration.DBConfiguration;
import org.ohdsi.databases.configuration.DBConfigurationException;
import org.ohdsi.databases.configuration.DbSettings;
import org.ohdsi.databases.configuration.DbType;

import java.sql.DriverManager;
import java.sql.SQLException;

public enum DatabricksHandler implements StorageHandler {
    INSTANCE();
    public static final String DATABRICKS_JDBC_CLASSNAME = "com.databricks.client.jdbc.Driver";
    private DBConnection databricksConnection = null;

    @Override
    public StorageHandler getInstance(DbSettings dbSettings) {
        if (databricksConnection == null) {
            databricksConnection = connectToDatabricks(dbSettings);
        }

        return INSTANCE;
    }

    @Override
    public DBConnection getDBConnection() {
        this.checkInitialised();
        return this.databricksConnection;
    }

    @Override
    public DbType getDbType() {
        return DbType.DATABRICKS;
    }

    @Override
    public String getTableSizeQuery(String tableName) {
        return String.format("SELECT COUNT(*) FROM %s;", resolveTableName(tableName));
    }

    @Override
    public void checkInitialised() throws DBConfigurationException {
        if (this.databricksConnection == null) {
            throw new DBConfigurationException("Snowflake DB/connection was not initialized");
        }
    }

    @Override
    public String getDatabase() {
        return "";
    }

    @Override
    public String getTablesQuery(String database) {
        return "SHOW TABLES IN default"; // TODO: this is hardcoded to the default database/schema
    }

    @Override
    public String getRowSampleQuery(String tableName, long rowCount, long sampleSize) {
        // SELECT * FROM test TABLESAMPLE (30 PERCENT) REPEATABLE (123);
        int percentage = (int) Math.ceil((double) sampleSize / rowCount * 100);
        if (percentage < 1) {
            percentage = 1;
        }
        String query = String.format("SELECT * FROM %s TABLESAMPLE (%d PERCENT) REPEATABLE (20250318)", resolveTableName(tableName), percentage);
        logger.info("Row sample query: {}", query);
        return query;
    }

    @Override
    public DBConfiguration getDBConfiguration() {
        return null;
    }

    private static DBConnection connectToDatabricks(DbSettings dbSettings) {
        try {
            Class.forName(DATABRICKS_JDBC_CLASSNAME);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Cannot find JDBC driver. Make sure the jar dependency for Databricks is available: " + ex.getMessage());
        }
        //String url = buildUrl(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, INSTANCE.configuration.getValue(SNOWFLAKE_AUTHENTICATOR));
        String url = System.getenv("WR_DATABRICKS_JDBC_URL"); // TODO: should be in dbSettings
        try {
            return new DBConnection(DriverManager.getConnection(url), DbType.DATABRICKS, true);
        } catch (SQLException ex) {
            throw new RuntimeException("Cannot connect to Databricks server: " + ex.getMessage());
        }
    }

    @Override
    public int getTableNameIndex() {
        return 1;
    }

    private String resolveTableName(String tableName) {
        return tableName;
    }
}
