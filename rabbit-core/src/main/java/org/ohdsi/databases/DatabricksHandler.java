package org.ohdsi.databases;

import org.apache.commons.lang.StringUtils;
import org.ohdsi.databases.configuration.*;
import org.ohdsi.utilities.collections.Pair;
import org.ohdsi.utilities.files.IniFile;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.ohdsi.databases.DatabricksHandler.DatabricksConfiguration.*;

public enum DatabricksHandler implements StorageHandler {
    INSTANCE();
    public static final String DATABRICKS_JDBC_CLASSNAME = "com.databricks.client.jdbc.Driver";
    private DBConnection databricksConnection = null;
    static DBConfiguration configuration = new DatabricksConfiguration();
    public static final String ERROR_NO_FIELD_OF_TYPE = "No value was specified for type";

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
            throw new DBConfigurationException("Databricks DB/connection was not initialized");
        }
    }

    @Override
    public String getDatabase() {
        return this.configuration.getValue(DATABRICKS_CATALOG);
    }

    public String getSchema() {
        return this.configuration.getValue(DATABRICKS_SCHEMA);
    }

    @Override
    public String getTablesQuery(String database) {
        return "SHOW TABLES IN default"; // TODO: this is hardcoded to the default database/schema
    }

    @Override
    public String getRowSampleQuery(String tableName, long rowCount, long sampleSize) {
        return getRowSampleQueryStaticForResolvedTableName(resolveTableName(tableName), rowCount, sampleSize);
    }

    public static String getRowSampleQueryStaticForResolvedTableName(String tableName, long rowCount, long sampleSize) {
        // sample query example: SELECT * FROM test TABLESAMPLE (30 PERCENT) REPEATABLE (123);
        int percentage = (int) Math.min(Math.max(Math.ceil((double) sampleSize / rowCount * 100), 1), 100);
        String query = String.format("SELECT * FROM %s TABLESAMPLE (%d PERCENT) REPEATABLE (20250318)", tableName, percentage);
        return query;
    }

    @Override
    public DBConfiguration getDBConfiguration() {
        return configuration;
    }

    private static DBConnection connectToDatabricks(DbSettings dbSettings) {
        try {
            Class.forName(DATABRICKS_JDBC_CLASSNAME);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Cannot find JDBC driver. Make sure the jar dependency for Databricks is available: " + ex.getMessage());
        }
        String url = buildUrl();
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

    public static class DatabricksConfiguration extends DBConfiguration {
        public static final String DATABRICKS_SERVER = "DATABRICKS_SERVER";
        public static final String TOOLTIP_DATABRICKS_SERVER = "Server for the Databricks instance";
        public static final String DATABRICKS_HTTP_PATH = "DATABRICKS_HTTP_PATH";
        public static final String DATABRICKS_PERSONAL_ACCESS_TOKEN = "DATABRICKS_PERSONAL_ACCESS_TOKEN";
        //public static final String SNOWFLAKE_AUTHENTICATOR = "SNOWFLAKE_AUTHENTICATOR";
        public static final String DATABRICKS_CATALOG = "DATABRICKS_CATALOG";
        //public static final String SNOWFLAKE_DATABASE = "SNOWFLAKE_DATABASE";
        public static final String DATABRICKS_SCHEMA = "DATABRICKS_SCHEMA";

        //public static final String ERROR_MUST_SET_PASSWORD_OR_AUTHENTICATOR = "Either password or authenticator must be specified for Snowflake";
        //public static final String ERROR_MUST_NOT_SET_PASSWORD_AND_AUTHENTICATOR = "Specify only one of password or authenticator Snowflake";
        //public static final String ERROR_VALUE_CAN_ONLY_BE_ONE_OF = "Error can only be one of ";
        public DatabricksConfiguration() {
            super(
                    ConfigurationField.create(
                                    DATABRICKS_SERVER,
                                    "Server",
                                    TOOLTIP_DATABRICKS_SERVER)
                            .required(),
                    ConfigurationField.create(
                                    DATABRICKS_HTTP_PATH,
                                    "HTTP path",
                                    "HTTP path for the Databricks instance")
                            .required(),
                    ConfigurationField.create(
                            DATABRICKS_PERSONAL_ACCESS_TOKEN,
                            "Personal Access Token",
                            "Personal Access Token for the Databricks instance"),
                    ConfigurationField.create(
                                    DATABRICKS_CATALOG,
                                    "Catalog",
                                    "Catalog for the Databricks instance"),
                    ConfigurationField.create(
                                    DATABRICKS_SCHEMA,
                                    "Schema",
                                    "Schema for the Databricks instance")
                    /* ConfigurationField.create(
                                    SNOWFLAKE_AUTHENTICATOR,
                                    "Authenticator method",
                                    "Snowflake JDBC authenticator method (only 'externalbrowser' is currently supported)")
                            .addValidator(new FieldValidator() {
                                private final List<String> allowedValues = Arrays.asList("externalbrowser");
                                @Override
                                public ValidationFeedback validate(ConfigurationField field) {
                                    ValidationFeedback feedback = new ValidationFeedback();
                                    if (StringUtils.isNotEmpty(field.getValue())) {
                                        if (!allowedValues.contains(field.getValue().toLowerCase())) {
                                            feedback.addError(String.format("%s (%s)", ERROR_VALUE_CAN_ONLY_BE_ONE_OF,
                                                    String.join(", ", allowedValues)), field);
                                        } else {
                                            field.setValue(field.getValue().toLowerCase());
                                        }
                                    }
                                    return feedback;
                                }
                            }) */
            );
            //this.configurationFields.addValidator(new DatabricksHandler.SnowflakeConfiguration.PasswordXORAuthenticatorValidator());
        }

        public DbSettings toDbSettings(ValidationFeedback feedback) {
            return getConfiguration(this.toIniFile(),feedback ).getItem2();
        }

    }

    public static Pair<DatabricksHandler.DatabricksConfiguration, DbSettings> getConfiguration(IniFile iniFile, ValidationFeedback feedback) {
        DatabricksHandler.DatabricksConfiguration configuration = new DatabricksHandler.DatabricksConfiguration();
        ValidationFeedback currentFeedback = configuration.loadAndValidateConfiguration(iniFile);
        if (feedback != null) {
            feedback.add(currentFeedback);
        }

        DbSettings dbSettings = new DbSettings();
        dbSettings.dbType = DbType.DATABRICKS;
        dbSettings.server = String.format("%s:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=%s;", configuration.getValue(DATABRICKS_SERVER), configuration.getValue(DATABRICKS_HTTP_PATH));
        dbSettings.database = String.format("%s.%s",
                configuration.getValue(DATABRICKS_CATALOG),
                configuration.getValue(DATABRICKS_SCHEMA));
        dbSettings.domain = dbSettings.database;
        dbSettings.user = "";
        dbSettings.password = configuration.getValue(DATABRICKS_PERSONAL_ACCESS_TOKEN);
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;

        return new Pair<>(configuration, dbSettings);
    }

    private static String buildUrl() {
        final String jdbcPrefix = "jdbc:databricks://";
        String server = configuration.getValue(DATABRICKS_SERVER);
        String url = (!server.startsWith(jdbcPrefix) ? jdbcPrefix : "") + server;
        url += ":443/default;transportMode=http;ssl=1;AuthMech=3";

        url = appendParameterIfSet(url, "httpPath", configuration.getValue(DATABRICKS_HTTP_PATH));
        url = appendParameterIfSet(url, "PWD", configuration.getValue(DATABRICKS_PERSONAL_ACCESS_TOKEN));
        url = appendParameterIfSet(url, "ConnCatalog", configuration.getValue(DATABRICKS_CATALOG));
        url = appendParameterIfSet(url, "ConnSchema", configuration.getValue(DATABRICKS_SCHEMA));
        /*if (!StringUtils.isEmpty(authenticator)) {
            url = appendParameterIfSet(url, "authenticator", authenticator);
        } else {
            url = appendParameterIfSet(url, "password", password);
        }*/

        // disable Apache Arrow as it causes issues with logging
        // see: https://stackoverflow.com/questions/74467671/azure-databricks-logging-configuration-problems
        url += ";EnableArrow=0";

        return url;
    }
    private static String appendParameterIfSet(String url, String name, String value) {
        if (!StringUtils.isEmpty(value)) {
            return String.format("%s%s%s=%s", url, (url.endsWith(";") ? "" : ";"), name, value);
        }
        return url;
        /*else {
            throw new RuntimeException(String.format(ERROR_NO_FIELD_OF_TYPE + " %s", name));
        }*/
    }

    private String resolveTableName(String tableName) {
        return tableName;
    }
}
