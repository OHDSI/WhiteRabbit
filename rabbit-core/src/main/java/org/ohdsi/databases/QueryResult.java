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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryResult implements Iterable<Row> {
    private String sql;

    private List<DBRowIterator> iterators = new ArrayList<>();
    private DBConnection dbConnection;

    public QueryResult(String sql, DBConnection dbConnection) {
        this(sql, dbConnection, false);
    }

    public QueryResult(String sql, DBConnection dbConnection, boolean verbose) {
        this.sql = sql;
        this.dbConnection = dbConnection;
    }

    @Override
    public Iterator<Row> iterator() {
        DBRowIterator iterator = new DBRowIterator(sql, dbConnection, false);
        iterators.add(iterator);
        return iterator;
    }

    public void close() {
        for (DBRowIterator iterator : iterators) {
            iterator.close();
        }
    }
}
