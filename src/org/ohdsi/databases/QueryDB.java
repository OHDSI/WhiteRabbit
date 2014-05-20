/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
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
 * 
 * @author Observational Health Data Sciences and Informatics
 * @author Martijn Schuemie
 ******************************************************************************/
package org.ohdsi.databases;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;

import org.ohdsi.utilities.files.ReadTextFile;
import org.ohdsi.utilities.files.Row;

public class QueryDB implements Iterable<Row>{
	private String sql;
	private Connection connection;

	public QueryDB(InputStream sqlStream, Connection connection){
		this(sqlStream,null,connection);
	}

	public QueryDB(InputStream sqlStream, Map<String, String> parameterToValue, Connection connection){
		this.connection = connection;
		sql = loadSQL(sqlStream);
		if (parameterToValue != null)
			for (String parameter : parameterToValue.keySet())
				sql = sql.replaceAll(parameter, parameterToValue.get(parameter));
		trimSQL();
	}

	public QueryDB(String sql, Connection connection){
		this.connection = connection;
		this.sql = sql;
		trimSQL();
	}

	private void trimSQL() {
		sql = sql.trim();
		if (sql.endsWith(";"))
			sql = sql.substring(0,sql.length()-1);
	}

	private String loadSQL(InputStream sqlStream) {
		StringBuilder sql = new StringBuilder();
		for (String line : new ReadTextFile(sqlStream)){
			sql.append(line.trim());
			sql.append('\n');
		}
		return sql.substring(0, sql.lastIndexOf(";"));
	}

	public boolean execute(){
		boolean result = false;
		for (String subQuery : sql.split(";"))
			try {
				Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				result = statement.execute(subQuery.toString());
			} catch (SQLException e) {
				if (e.getMessage().contains("ORA-00942") && sql.toUpperCase().startsWith("DROP"))
					System.out.println("Tried dropping non-existing table");
				else {	
					System.err.println("SQL: " + subQuery);
					e.printStackTrace();
				}
			}
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public Iterator<Row> iterator() {
		return new DBRowIterator(sql);
	}
	public class DBRowIterator implements Iterator<Row>{

		private ResultSet resultSet;

		private boolean hasNext;

		public DBRowIterator(String sql) {
			try {
				Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				resultSet = statement.executeQuery(sql.toString());
				hasNext = resultSet.next();
				if (!hasNext)
					connection.close();
			} catch (SQLException e){
				System.err.println(sql.toString());
				System.err.println(e.getMessage());
				throw new RuntimeException(e);
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
				for (int i = 1; i < metaData.getColumnCount()+1; i++) {
					String columnName = metaData.getColumnName(i);
					String value = resultSet.getString(i);
					if (value == null)
						value = "";
					row.add(columnName, value.replace(" 00:00:00", ""));
				}
				hasNext = resultSet.next();
				if (!hasNext)
					connection.close();
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
}
