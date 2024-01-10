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

import org.ohdsi.utilities.files.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

class DBRowIterator implements Iterator<Row> {
    static Logger logger = LoggerFactory.getLogger(DBRowIterator.class);

    private ResultSet resultSet;

    private boolean hasNext;

    private Set<String> columnNames = new HashSet<>();

    public DBRowIterator(String sql, RichConnection richConnection) {
        new DBRowIterator(sql, richConnection.getConnection(), richConnection.isVerbose());
    }
    public DBRowIterator(String sql, DBConnection dbConnection, boolean verbose) {
        Statement statement;
        try {
            sql.trim();
            if (sql.endsWith(";"))
                sql = sql.substring(0, sql.length() - 1);
            if (verbose) {
                String abbrSQL = sql.replace('\n', ' ').replace('\t', ' ').trim();
                if (abbrSQL.length() > 100)
                    abbrSQL = abbrSQL.substring(0, 100).trim() + "...";
                logger.info("Executing query: {}", abbrSQL);
            }
            long start = System.currentTimeMillis();
            statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            resultSet = statement.executeQuery(sql);
            hasNext = resultSet.next();
            if (verbose)
                dbConnection.outputQueryStats(statement, System.currentTimeMillis() - start);
        } catch (SQLException e) {
            logger.error(sql, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            resultSet = null;
            hasNext = false;
        }
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public Row next() {
        try {
            Row row = new Row();
            ResultSetMetaData metaData;
            metaData = resultSet.getMetaData();
            columnNames.clear();

            for (int i = 1; i < metaData.getColumnCount() + 1; i++) {
                String columnName = metaData.getColumnName(i);
                if (columnNames.add(columnName)) {
                    String value;
                    try {
                        value = resultSet.getString(i);
                    } catch (Exception e) {
                        value = "";
                    }
                    if (value == null)
                        value = "";

                    row.add(columnName, value.replace(" 00:00:00", ""));
                }
            }
            hasNext = resultSet.next();
            if (!hasNext) {
                resultSet.close();
                resultSet = null;
            }
            return row;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
    }
}
