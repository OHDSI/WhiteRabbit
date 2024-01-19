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

import org.ohdsi.databases.configuration.*;
import org.ohdsi.utilities.files.IniFile;
import org.ohdsi.utilities.files.Row;

import java.io.PrintStream;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * StorageHandler defines the interface that a database connection class must implement.
 *
 */
public interface StorageHandler {

    /**
     * Creates an instance of the implementing class, or can return the singleton for.
     *
     * @param dbSettings Configuration parameters for the implemented database
     * @return instance of a StorageHandler implementing class
     */
    StorageHandler getInstance(DbSettings dbSettings);

    /**
     * Returns the DBConnection object associated with the database connection
     *
     * @return DBConnection object
     */
    DBConnection getDBConnection();

    /**
     * @return the DbType enum constant associated with the implementation
     */
    DbType getDbType();

    /**
     *
     * @param tableName name of the table to get the size (number of rows) for
     * @return Implementation specific query to get the size of the table
     */
    String getTableSizeQuery(String tableName);

    /**
     * Verifies if the implementing object was properly configured for use. Should throw a DBConfigurationException
     * if this is not the case.
     *
     * @throws DBConfigurationException Object not ready for use
     */
    void checkInitialised() throws DBConfigurationException;

    /**
     * Returns the row count of the specified table.
     *
     * @param tableName name of table
     * @return size of table in rows
     */
    default long getTableSize(String tableName ) {
        long returnVal;
        QueryResult qr = new QueryResult(getTableSizeQuery(tableName), getDBConnection());
        try {
            returnVal = Long.parseLong(qr.iterator().next().getCells().get(0));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            qr.close();
        }
        return returnVal;
    }

    /**
     * Executes an SQL use statement (or similar) if the underlying database requires it.
     *
     * No-op by default.
     *
     * @param ignoredDatabase provided for compatibility
     */
    default void use(String ignoredDatabase) {}

    /**
     * closes the connection to the database. No-op by default.
     */
    default void close() {
        // no-op by default, so singletons don't need to implement it
    }

    /**
     * Returns the name of the database the connection was initiated for.
     *
     * @return name of (current) database
     */
    String getDatabase();

    /**
     *
     * @return List of table names in current database
     */
    default List<String> getTableNames() {
        List<String> names = new ArrayList<>();
        String query = this.getTablesQuery(getDatabase());

		for (Row row : new QueryResult(query, new DBConnection(this, getDbType(), false))) {
            names.add(row.getCells().get(0));
        }

        return names;
    }

    /**
     * Fetches the structure of a table as a list of FieldInfo objects.
     *
     * The default implementation should work for some/most/all JDBC databases and only needs to be overridden
     * for databases where this is not the case.
     *
     * @param table name of the table to fetch the structure for
     * @param scanParameters parameters that are to be used for scanning the table
     * @return
     */
    default List<FieldInfo> fetchTableStructure(String table, ScanParameters scanParameters) {
        List<FieldInfo> fieldInfos = new ArrayList<>();
        ResultSet rs = getFieldNamesFromJDBC(table);
        try {
            while (rs.next()) {
                FieldInfo fieldInfo = new FieldInfo(scanParameters, rs.getString("COLUMN_NAME"));
                fieldInfo.type = rs.getString("TYPE_NAME");
                fieldInfo.rowCount = getTableSize(table);
                fieldInfos.add(fieldInfo);
            }
        } catch (
                SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
        return fieldInfos;
    }

    /**
     * Retrieves column names (fields) for a table.
     *
     * The default implementation uses the JDBC metadata. Should only be overridden if this approach does not work
     * for the underlying database.
     *
     * @param table name of the table to get the column names for
     * @return java.sql.ResultSet
     */
    default ResultSet getFieldNamesFromJDBC(String table) {
        try {
            DatabaseMetaData metadata = getDBConnection().getMetaData();
            return metadata.getColumns(null, null, table, null);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Returns the database specific query to obtain the table names in the database.
     * See getTableNames(), which calls this method
     *
     * @param database
     * @return
     */
    String getTablesQuery(String database);

    /**
     * Returns the database specific query that should be used to obtain a sample of rows from a table.
     *
     * @param table table to get sample from
     * @param rowCount known rowcount for the table
     * @param sampleSize size of the sample
     * @return Database specific SQL query
     */
    String getRowSampleQuery(String table, long rowCount, long sampleSize);

    /**
     * @return the DbSettings object used to initialize the database connection
     */
    default DbSettings getDbSettings(ValidationFeedback feedback) {
        return getDBConfiguration().toDbSettings(feedback);
    }

    /**
     * Returns a validated DbSettings object with values based on the IniFile object
     *
     * @param iniFile IniFile object containing database configuration values for the class
     *                that implements the StorageHandler
     *
     * @return DbSettings object
     */
    default DbSettings getDbSettings(IniFile iniFile, ValidationFeedback feedback, PrintStream outStream) {
        ValidationFeedback validationFeedback = getDBConfiguration().loadAndValidateConfiguration(iniFile);
        if (feedback != null) {
            feedback.add(validationFeedback);
        }
        if (outStream != null) {
            if (validationFeedback.hasErrors()) {
                outStream.println("There are errors for the configuration file:");
                validationFeedback.getErrors().forEach((error, fields) ->
                        outStream.printf("\t%s (%s)%n", error, fields.stream().map(f -> f.name).collect(Collectors.joining(","))));
            }
            if (validationFeedback.hasWarnings()) {
                outStream.println("There are errors for the configuration file:");
                validationFeedback.getWarnings().forEach((warning, fields) ->
                        outStream.printf("\t%s (%s)%n", warning, fields.stream().map(f -> f.name).collect(Collectors.joining(","))));
            }
        }
        return getDBConfiguration().toDbSettings(feedback);
    }

    /**
     * Returns the DBConfiguration object for the implementing class
     */
    DBConfiguration getDBConfiguration();
}
