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
package org.ohdsi.databases;

import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

import org.ohdsi.databases.configuration.*;
import org.ohdsi.utilities.collections.Pair;
import org.ohdsi.utilities.files.IniFile;

import static org.ohdsi.databases.SnowflakeHandler.SnowflakeConfiguration.*;

/*
 * SnowflakeHandler implements all Snowflake specific logic required to connect to, and query, a Snowflake instance.
 *
 * It is implemented as a Singleton, using the enum pattern es described here: https://www.baeldung.com/java-singleton
 */
public enum SnowflakeHandler implements JdbcStorageHandler {
    INSTANCE();
    public static final String WR_USE_SNOWFLAKE_JDBC_METADATA = "WR_USE_SNOWFLAKE_METADATA";

    ScanConfiguration configuration = new SnowflakeConfiguration();
    private DBConnection snowflakeConnection = null;

    private final DbType dbType = DbType.SNOWFLAKE;
    public static final String ERROR_NO_FIELD_OF_TYPE = "No value was specified for type";
    public static final String ERROR_INCORRECT_SCHEMA_SPECIFICATION =
            "Database should be specified as 'warehouse.database.schema', " +
                    "e.g. 'computewh.snowflake_sample_data.weather";
    public static final String ERROR_CONNECTION_NOT_INITIALIZED =
            "Snowflake Database connection has not been initialized.";

    SnowflakeHandler() {
    }

    public void resetConnection() throws SQLException {
        if (this.snowflakeConnection != null) {
            this.snowflakeConnection.close();
        }
        this.snowflakeConnection = null;
    }

    @Override
    public JdbcStorageHandler getInstance(DbSettings dbSettings) {
        if (snowflakeConnection == null) {
            snowflakeConnection = connectToSnowflake(dbSettings);
        }

        return INSTANCE;
    }

    public static Pair<SnowflakeConfiguration, DbSettings> getConfiguration(IniFile iniFile, ValidationFeedback feedback) {
        SnowflakeConfiguration configuration = new SnowflakeConfiguration();
        ValidationFeedback currentFeedback = configuration.loadAndValidateConfiguration(iniFile);
        if (feedback != null) {
            feedback.add(currentFeedback);
        }

        String warehouse = configuration.getValue(SNOWFLAKE_WAREHOUSE);
        DbSettings dbSettings = new DbSettings();
        dbSettings.dbType = DbType.SNOWFLAKE;
        dbSettings.server = String.format("https://%s.snowflakecomputing.com", configuration.getValue(SNOWFLAKE_ACCOUNT));
        dbSettings.database = String.format("%s.%s.%s",
                warehouse,
                configuration.getValue(SNOWFLAKE_DATABASE),
                configuration.getValue(SNOWFLAKE_SCHEMA));
        dbSettings.domain = dbSettings.database;
        dbSettings.user = configuration.getValue(SNOWFLAKE_USER);
        dbSettings.password = configuration.getValue(SNOWFLAKE_PASSWORD);
        dbSettings.sourceType = DbSettings.SourceType.DATABASE;

        return new Pair<>(configuration, dbSettings);
    }

    public DBConnection getDBConnection() {
        this.checkInitialised();
        return this.snowflakeConnection;
    }

    public String getUseQuery(String ignoredDatabase) {
        String useQuery = String.format("USE WAREHOUSE %s;", configuration.getValue(SNOWFLAKE_WAREHOUSE));
        logger.info("SnowFlakeHandler will execute query: {}", useQuery);
        return useQuery;
    }

    @Override
    public String getTableSizeQuery(String tableName) {
        return String.format("SELECT COUNT(*) FROM %s;", resolveTableName(tableName));
    }

    public String getRowSampleQuery(String tableName, long rowCount, long sampleSize) {
        return String.format("SELECT * FROM %s ORDER BY RANDOM() LIMIT %s", resolveTableName(tableName), sampleSize);
    }

    private String resolveTableName(String tableName) {
        return String.format("%s.%s.%s", this.getDatabase(), this.getSchema(), tableName);
    }

    @Override
    public ResultSet getFieldsInformation(String tableName) {
        try {
            String database = this.getDatabase();
            String schema = this.getSchema();
            DatabaseMetaData metadata = getDBConnection().getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                database = database.toUpperCase();
                schema = schema.toUpperCase();
                tableName = tableName.toUpperCase();
            } else if (metadata.storesLowerCaseIdentifiers()) {
                database = database.toLowerCase();
                schema = schema.toLowerCase();
                tableName = tableName.toLowerCase();
            }

            logger.warn("Obtaining columnn information from JDBC metadata: metadata.getColumns({}, {}, {}, null)",
                    database, schema, tableName);
            return metadata.getColumns(database, schema, tableName, null);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public String getFieldsInformationQuery(String tableName) {
        if (System.getenv(WR_USE_SNOWFLAKE_JDBC_METADATA) != null || System.getProperty(WR_USE_SNOWFLAKE_JDBC_METADATA) != null) {
            return null;    // not providing a query forces use of JDBC metadata
        } else {
            return String.format(
                    "SELECT column_name, data_type FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                    this.getDatabase().toUpperCase(), this.getSchema().toUpperCase(), tableName.toUpperCase());
        }
    }

    public String getTablesQuery(String database) {
        return String.format("SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s'", this.getDatabase().toUpperCase(), this.getSchema().toUpperCase());
    }

    @Override
    public void checkInitialised() throws ScanConfigurationException {
        if (this.snowflakeConnection == null) {
            throw new ScanConfigurationException("Snowflake DB/connection was not initialized");
        }
    }

    public DbType getDbType() {
        return this.dbType;
    }

    private static DBConnection connectToSnowflake(DbSettings dbSettings) {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Cannot find JDBC driver. Make sure the file snowflake-jdbc-x.xx.xx.jar is in the path: " + ex.getMessage());
        }
        String url = buildUrl(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, INSTANCE.configuration.getValue(SNOWFLAKE_AUTHENTICATOR));
        try {
            return new DBConnection(DriverManager.getConnection(url), DbType.SNOWFLAKE, false);
        } catch (SQLException ex) {
            throw new RuntimeException("Cannot connect to Snowflake server: " + ex.getMessage());
        }
    }

    public ResultSet getFieldNames(String table) {
        try {
            DatabaseMetaData metadata = this.snowflakeConnection.getMetaData();
            return metadata.getColumns(null, null, table, null);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public ScanConfiguration getScanConfiguration() {

        return this.configuration;
    }

    public static class SnowflakeConfiguration extends ScanConfiguration {
        public static final String SNOWFLAKE_ACCOUNT = "SNOWFLAKE_ACCOUNT";
        public static final String TOOLTIP_SNOWFLAKE_ACCOUNT = "Account for the Snowflake instance";
        public static final String SNOWFLAKE_USER = "SNOWFLAKE_USER";
        public static final String SNOWFLAKE_PASSWORD = "SNOWFLAKE_PASSWORD";
        public static final String SNOWFLAKE_AUTHENTICATOR = "SNOWFLAKE_AUTHENTICATOR";
        public static final String SNOWFLAKE_WAREHOUSE = "SNOWFLAKE_WAREHOUSE";
        public static final String SNOWFLAKE_DATABASE = "SNOWFLAKE_DATABASE";
        public static final String SNOWFLAKE_SCHEMA = "SNOWFLAKE_SCHEMA";
        public static final String ERROR_MUST_SET_PASSWORD_OR_AUTHENTICATOR = "Either password or authenticator must be specified for Snowflake";
        public static final String ERROR_MUST_NOT_SET_PASSWORD_AND_AUTHENTICATOR = "Specify only one of password or authenticator Snowflake";
        public static final String ERROR_VALUE_CAN_ONLY_BE_ONE_OF = "Error can only be one of ";
        public SnowflakeConfiguration() {
            super(
                ConfigurationField.create(
                        SNOWFLAKE_ACCOUNT,
                        "Account",
                        TOOLTIP_SNOWFLAKE_ACCOUNT)
                        .required(),
                ConfigurationField.create(
                        SNOWFLAKE_USER,
                        "User",
                        "User for the Snowflake instance")
                        .required(),
                ConfigurationField.create(
                        SNOWFLAKE_PASSWORD,
                        "Password",
                        "Password for the Snowflake instance"),
                ConfigurationField.create(
                        SNOWFLAKE_WAREHOUSE,
                        "Warehouse",
                        "Warehouse for the Snowflake instance")
                        .required(),
                ConfigurationField.create(
                        SNOWFLAKE_DATABASE,
                        "Database",
                        "Database for the Snowflake instance")
                        .required(),
                ConfigurationField.create(
                        SNOWFLAKE_SCHEMA,
                        "Schema",
                        "Schema for the Snowflake instance")
                        .required(),
                ConfigurationField.create(
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
                        })
            );
            this.configurationFields.addValidator(new PasswordXORAuthenticatorValidator());
        }

        static class PasswordXORAuthenticatorValidator implements ConfigurationValidator {

            @Override
            public ValidationFeedback validate(ConfigurationFields fields) {
                ValidationFeedback feedback = new ValidationFeedback();
                String password = fields.getValue(SNOWFLAKE_PASSWORD);
                String authenticator = fields.getValue(SNOWFLAKE_AUTHENTICATOR);
                if (StringUtils.isEmpty(password) && StringUtils.isEmpty(authenticator)) {
                    feedback.addError(ERROR_MUST_SET_PASSWORD_OR_AUTHENTICATOR, fields.get(SNOWFLAKE_PASSWORD));
                    feedback.addError(ERROR_MUST_SET_PASSWORD_OR_AUTHENTICATOR, fields.get(SNOWFLAKE_AUTHENTICATOR));
                } else if (!StringUtils.isEmpty(password) && !StringUtils.isEmpty(authenticator)) {
                    feedback.addError(ERROR_MUST_NOT_SET_PASSWORD_AND_AUTHENTICATOR, fields.get(SNOWFLAKE_PASSWORD));
                    feedback.addError(ERROR_MUST_NOT_SET_PASSWORD_AND_AUTHENTICATOR, fields.get(SNOWFLAKE_AUTHENTICATOR));
                }

                return feedback;
            }
        }
        @Override
        public DbSettings toDbSettings(ValidationFeedback feedback) {
            return getConfiguration(this.toIniFile(),feedback ).getItem2();
        }

    }

    private static String buildUrl(String server, String schema, String user, String password, String authenticator) {
        final String jdbcPrefix = "jdbc:snowflake://";
        String url = (!server.startsWith(jdbcPrefix) ? jdbcPrefix : "") + server;
        if (!url.contains("?")) {
            url += "?";
        }

        String[] parts = splitDatabaseName(schema);
        url = appendParameterIfSet(url, "warehouse", parts[0]);
        url = appendParameterIfSet(url, "db", parts[1]);
        url = appendParameterIfSet(url, "schema", parts[2]);
        url = appendParameterIfSet(url, "user", user);
        if (!StringUtils.isEmpty(authenticator)) {
            url = appendParameterIfSet(url, "authenticator", authenticator);
        } else {
            url = appendParameterIfSet(url, "password", password);
        }

        return url;
    }
    private static String appendParameterIfSet(String url, String name, String value) {
        if (!StringUtils.isEmpty(value)) {
            return String.format("%s%s%s=%s", url, (url.endsWith("?") ? "" : "&"), name, value);
        }
        else {
            throw new RuntimeException(String.format(ERROR_NO_FIELD_OF_TYPE + " %s", name));
        }
    }
    private static String[] splitDatabaseName(String databaseName) {
        String[] parts = databaseName.split("\\.");
        if (parts.length != 3) {
            throw new RuntimeException(ERROR_INCORRECT_SCHEMA_SPECIFICATION);
        }

        return parts;
    }

    public String getDatabase() {
        return this.configuration.getValue(SNOWFLAKE_DATABASE);
    }

    private String getSchema() {
        return this.configuration.getValue(SNOWFLAKE_SCHEMA);
    }
}
