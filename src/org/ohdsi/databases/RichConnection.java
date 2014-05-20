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
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ohdsi.utilities.SimpleCounter;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.ReadTextFile;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteCSVFileWithHeader;

public class RichConnection {
	public static int				INSERT_BATCH_SIZE	= 100000;
	private Connection				connection;
	private Class<?>				context;												// Used for locating resources
	private boolean					verbose				= false;
	private static DecimalFormat	decimalFormat		= new DecimalFormat("#.#");
	private Map<String, String>		localVariables		= new HashMap<String, String>();
	private DbType					dbType;
	
	public RichConnection(String server, String domain, String user, String password, DbType dbType) {
		this.connection = DBConnector.connect(server, domain, user, password, dbType);
		this.dbType = dbType;
	}
	
	/**
	 * Executes the SQL statement specified in the resource. The first parameter string is the parameter name, the second string the value, etc.
	 * 
	 * @param resourceName
	 * @param parameters
	 */
	public void executeResource(String resourceName, Object... parameters) {
		QueryParameters parameterMap = new QueryParameters();
		for (int i = 0; i < parameters.length; i += 2)
			parameterMap.set(parameters[i].toString(), parameters[i + 1].toString());
		executeResource(resourceName, parameterMap);
	}
	
	/**
	 * Executes the SQL statement specified in the resource
	 * 
	 * @param resourceName
	 * @param parameters
	 */
	public void executeResource(String resourceName, QueryParameters parameters) {
		executeResource(context.getResourceAsStream(resourceName), parameters);
	}
	
	/**
	 * Executes the SQL statement specified in the resource
	 * 
	 * @param sqlStream
	 * @param parameters
	 */
	public void executeResource(InputStream sqlStream, QueryParameters parameters) {
		String sql = loadSQL(sqlStream);
		if (parameters != null)
			sql = applyVariables(sql, parameters.getMap());
		execute(sql);
	}
	
	/**
	 * Executes the SQL statement specified in the resource
	 * 
	 * @param sqlStream
	 */
	public void executeResource(InputStream sqlStream) {
		executeResource(sqlStream, null);
	}
	
	/**
	 * Executes the SQL statement specified in the resource
	 * 
	 * @param sql
	 */
	public void executeResource(String sql) {
		if (context == null)
			throw new RuntimeException("Context not specified, unable to load resource");
		executeResource(context.getResourceAsStream(sql));
	}
	
	public void executeAsOne(String sql) {
		try {
			Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.execute(sql);
		} catch (SQLException e) {
			System.err.println(sql);
			e.printStackTrace();
		}
	}
	
	/**
	 * Execute the given SQL statement.
	 * 
	 * @param sql
	 */
	public void execute(String sql) {
		try {
			sql = handleSQLDefineStatements(sql);
			if (sql.length() == 0)
				return;
			Statement statement = null;
			
			statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			for (String subQuery : sql.split(";")) {
				if (verbose) {
					String abbrSQL = subQuery.replace('\n', ' ').replace('\t', ' ').trim();
					if (abbrSQL.length() > 100)
						abbrSQL = abbrSQL.substring(0, 100).trim() + "...";
					System.out.println("Adding query to batch: " + abbrSQL);
				}
				
				statement.addBatch(subQuery);
			}
			long start = System.currentTimeMillis();
			if (verbose)
				System.out.println("Executing batch");
			statement.executeBatch();
			if (verbose)
				outputQueryStats(statement, System.currentTimeMillis() - start);
			statement.close();
		} catch (SQLException e) {
			System.err.println(sql);
			e.printStackTrace();
		}
	}
	
	private String handleSQLDefineStatements(String sql) {
		sql = extractSQLDefineStatements(sql);
		sql = applyVariables(sql, localVariables);
		return sql;
	}
	
	private String applyVariables(String sql, Map<String, String> key2Value) {
		List<String> sortedKeys = new ArrayList<String>(key2Value.keySet());
		Collections.sort(sortedKeys, new Comparator<String>() {
			
			@Override
			public int compare(String o1, String o2) {
				if (o1.length() > o2.length())
					return 1;
				else if (o1.length() < o2.length())
					return -1;
				else
					return 0;
			}
		});
		for (String key : sortedKeys)
			sql = sql.replaceAll(key, key2Value.get(key));
		return sql.trim();
	}
	
	private String extractSQLDefineStatements(String sql) {
		int start = sql.toLowerCase().indexOf("sqldefine");
		
		while (start != -1) {
			int end = sql.indexOf(";", start);
			if (end == -1)
				throw new RuntimeException("No closing semicolon found for SQLDEFINE in:\n" + sql);
			
			String definition = sql.substring(start, end);
			int as = definition.toLowerCase().indexOf(" as");
			if (as == -1)
				as = definition.toLowerCase().indexOf("\nas");
			if (as == -1)
				as = definition.toLowerCase().indexOf("\tas");
			if (as == -1)
				throw new RuntimeException("No AS found for SQLDEFINE in:\n" + sql);
			String variableName = definition.substring("SQLDEFINE".length(), as).trim();
			String variableValue = definition.substring(as + 3).trim();
			variableValue = applyVariables(variableValue, localVariables);
			if (verbose)
				System.out.println("Found definition for " + variableName);
			localVariables.put(variableName, variableValue);
			sql = sql.substring(0, start) + " " + sql.substring(end + 1);
			
			start = sql.toLowerCase().indexOf("sqldefine");
		}
		return sql;
	}
	
	private void outputQueryStats(Statement statement, long ms) throws SQLException {
		Throwable warning = statement.getWarnings();
		if (warning != null)
			System.out.println("- SERVER: " + warning.getMessage());
		String timeString;
		if (ms < 1000)
			timeString = ms + " ms";
		else if (ms < 60000)
			timeString = decimalFormat.format(ms / 1000d) + " seconds";
		else if (ms < 3600000)
			timeString = decimalFormat.format(ms / 60000d) + " minutes";
		else
			timeString = decimalFormat.format(ms / 3600000d) + " hours";
		System.out.println("- Query completed in " + timeString);
	}
	
	public QueryResult queryResource(String resourceName) {
		return queryResource(resourceName, new QueryParameters());
	}
	
	public QueryResult queryResource(String resourceName, QueryParameters parameters) {
		if (context == null)
			throw new RuntimeException("Context not specified, unable to load resource");
		return queryResource(context.getResourceAsStream(resourceName), parameters);
	}
	
	/**
	 * Query the database using the SQL statement specified in the resource
	 * 
	 * @param sqlStream
	 * @param parameters
	 * @return
	 */
	public QueryResult queryResource(InputStream sqlStream, QueryParameters parameters) {
		String sql = loadSQL(sqlStream);
		sql = applyVariables(sql, parameters.getMap());
		return query(sql);
	}
	
	/**
	 * Query the database using the SQL statement specified in the resource
	 * 
	 * @param sqlStream
	 * @return
	 */
	public QueryResult queryResource(InputStream sqlStream) {
		return queryResource(sqlStream, null);
	}
	
	/**
	 * Query the database using the provided SQL statement.
	 * 
	 * @param sql
	 * @return
	 */
	public QueryResult query(String sql) {
		return new QueryResult(sql);
	}
	
	/**
	 * Switch the database to use.
	 * 
	 * @param database
	 */
	public void use(String database) {
		if (database == null)
			return;
		if (dbType == DbType.ORACLE)
			execute("ALTER SESSION SET current_schema = " + database);
		else if (dbType == DbType.POSTGRESQL)
			execute("SET search_path TO " + database);
		else
			execute("USE " + database);
	}
	
	public List<String> getDatabaseNames() {
		List<String> names = new ArrayList<String>();
		String query = null;
		if (dbType == DbType.MYSQL)
			query = "SHOW DATABASES";
		else if (dbType == DbType.MSSQL)
			query = "SELECT name FROM master..sysdatabases";
		else
			throw new RuntimeException("Database type not supported");
		
		for (Row row : query(query))
			names.add(row.get(row.getFieldNames().get(0)));
		return names;
	}
	
	public List<String> getTableNames(String database) {
		List<String> names = new ArrayList<String>();
		String query = null;
		if (dbType == DbType.MYSQL) {
			if (database == null)
				query = "SHOW TABLES";
			else
				query = "SHOW TABLES IN " + database;
		} else if (dbType == DbType.MSSQL) {
			query = "SELECT name FROM " + database + ".sys.tables ";
		} else if (dbType == DbType.ORACLE) {
			query = "SELECT table_name FROM all_tables WHERE owner='" + database.toUpperCase() + "'";
		} else if (dbType == DbType.POSTGRESQL) {
			query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + database + "'";
		}
		
		for (Row row : query(query))
			names.add(row.get(row.getFieldNames().get(0)));
		return names;
	}
	
	public List<String> getFieldNames(String table) {
		List<String> names = new ArrayList<String>();
		if (dbType == DbType.MSSQL) {
			for (Row row : query("SELECT name FROM syscolumns WHERE id=OBJECT_ID('" + table + "')"))
				names.add(row.get("name"));
		} else if (dbType == DbType.MYSQL)
			for (Row row : query("SHOW COLUMNS FROM " + table))
				names.add(row.get("COLUMN_NAME"));
		else
			throw new RuntimeException("DB type not supported");
		
		return names;
	}
	
	public void dropTableIfExists(String table) {
		if (dbType == DbType.ORACLE) {
			try {
				Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				if (verbose) {
					System.out.println("Executing: TRUNCATE TABLE " + table);
				}
				statement.execute("TRUNCATE TABLE " + table);
				if (verbose) {
					System.out.println("Executing: DROP TABLE " + table);
				}
				statement.execute("DROP TABLE " + table);
				statement.close();
			} catch (Exception e) {
				if (verbose)
					System.out.println(e.getMessage());
			}
		} else if (dbType == DbType.MSSQL) {
			execute("IF OBJECT_ID('" + table + "', 'U') IS NOT NULL DROP TABLE " + table + ";");
		} else {
			execute("DROP TABLE " + table + " IF EXISTS");
		}
	}
	
	public void dropDatabaseIfExists(String database) {
		execute("DROP DATABASE " + database);
	}
	
	/**
	 * Returns the row count of the specified table.
	 * 
	 * @param tableName
	 * @return
	 */
	public long getTableSize(String tableName) {
		if (dbType == DbType.MSSQL)
			return Long.parseLong(query("SELECT COUNT(*) FROM [" + tableName + "];").iterator().next().getCells().get(0));
		else
			return Long.parseLong(query("SELECT COUNT(*) FROM " + tableName + ";").iterator().next().getCells().get(0));
	}
	
	/**
	 * Close the connection to the database.
	 */
	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public boolean isVerbose() {
		return verbose;
	}
	
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	private String loadSQL(InputStream sqlStream) {
		StringBuilder sql = new StringBuilder();
		for (String line : new ReadTextFile(sqlStream)) {
			line = line.replaceAll("--.*", ""); // Remove comments
			line = line.trim();
			if (line.length() != 0) {
				sql.append(line.trim());
				sql.append('\n');
			}
		}
		return sql.toString();
	}
	
	public Class<?> getContext() {
		return context;
	}
	
	public void setContext(Class<?> context) {
		this.context = context;
	}
	
	public class QueryResult implements Iterable<Row> {
		private String				sql;
		
		private List<DBRowIterator>	iterators	= new ArrayList<DBRowIterator>();
		
		public QueryResult(String sql) {
			this.sql = sql;
		}
		
		@Override
		public Iterator<Row> iterator() {
			DBRowIterator iterator = new DBRowIterator(sql);
			iterators.add(iterator);
			return iterator;
		}
		
		public void close() {
			for (DBRowIterator iterator : iterators) {
				iterator.close();
			}
		}
	}
	
	/**
	 * Writes the results of a query to the specified file in CSV format.
	 * 
	 * @param queryResult
	 * @param filename
	 */
	public void writeToFile(QueryResult queryResult, String filename) {
		WriteCSVFileWithHeader out = new WriteCSVFileWithHeader(filename);
		for (Row row : queryResult)
			out.write(row);
		out.close();
	}
	
	/**
	 * Reads a table from a file in CSV format, and inserts it into the database.
	 * 
	 * @param filename
	 * @param table
	 * @param create
	 *            If true, the data format is determined based on the first batch of rows and used to create the table structure.
	 */
	public void readFromFile(String filename, String table, boolean create) {
		insertIntoTable(new ReadCSVFileWithHeader(filename).iterator(), table, create);
	}
	
	/**
	 * Inserts the rows into a table in the database.
	 * 
	 * @param iterator
	 * @param tableName
	 * @param create
	 *            If true, the data format is determined based on the first batch of rows and used to create the table structure.
	 */
	public void insertIntoTable(Iterator<Row> iterator, String table, boolean create) {
		List<Row> batch = new ArrayList<Row>(INSERT_BATCH_SIZE);
		
		boolean first = true;
		SimpleCounter counter = new SimpleCounter(1000000, true);
		while (iterator.hasNext()) {
			if (batch.size() == INSERT_BATCH_SIZE) {
				if (first && create)
					createTable(table, batch);
				insert(table, batch);
				batch.clear();
				first = false;
			}
			batch.add(iterator.next());
			counter.count();
		}
		if (batch.size() != 0) {
			if (first && create)
				createTable(table, batch);
			insert(table, batch);
		}
	}
	
	// private Map<String, String> getNumericFieldsInPostgreSQL(String table) {
	// Map<String, String> fieldToType = new HashMap<String, String>();
	// for (Row row : query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '" + table + "'"))
	// fieldToType.put(row.get("column_name"), row.get("data_type"));
	// return fieldToType;
	// }
	
	private void insert(String tableName, List<Row> rows) {
		List<String> columns = null;
		columns = rows.get(0).getFieldNames();
		for (int i = 0; i < columns.size(); i++)
			columns.set(i, columnNameToSqlName(columns.get(i)));
		
		String sql = "INSERT INTO " + tableName;
		sql = sql + " (" + StringUtilities.join(columns, ",") + ")";
		sql = sql + " VALUES (?";
		for (int i = 1; i < columns.size(); i++)
			sql = sql + ",?";
		sql = sql + ")";
		try {
			connection.setAutoCommit(false);
			PreparedStatement statement = connection.prepareStatement(sql);
			for (Row row : rows) {
				for (int i = 0; i < columns.size(); i++) {
					String value = row.get(columns.get(i));
					if (value == null)
						System.out.println(row.toString());
					if (value.length() == 0)
						value = null;
					// System.out.println(value);
					if (dbType == DbType.POSTGRESQL) // PostgreSQL does not allow unspecified types
						statement.setObject(i + 1, value, Types.OTHER);
					else if (dbType == DbType.ORACLE) {
						if (isDate(value)) {
							// System.out.println(value);
							statement.setDate(i + 1, java.sql.Date.valueOf(value));
							
						} else
							statement.setString(i + 1, value);
					} else
						statement.setString(i + 1, value);
				}
				statement.addBatch();
			}
			statement.executeBatch();
			connection.commit();
			statement.close();
			connection.setAutoCommit(true);
			connection.clearWarnings();
		} catch (SQLException e) {
			e.printStackTrace();
			if (e instanceof BatchUpdateException) {
				System.err.println(((BatchUpdateException) e).getNextException().getMessage());
			}
		}
	}
	
	private static boolean isDate(String string) {
		if (string != null && string.length() == 10 && string.charAt(4) == '-' && string.charAt(7) == '-')
			try {
				int year = Integer.parseInt(string.substring(0, 4));
				if (year < 1700 || year > 2200)
					return false;
				int month = Integer.parseInt(string.substring(5, 7));
				if (month < 1 || month > 12)
					return false;
				int day = Integer.parseInt(string.substring(8, 10));
				if (day < 1 || day > 31)
					return false;
				return true;
			} catch (Exception e) {
				return false;
			}
		return false;
	}
	
	private Set<String> createTable(String tableName, List<Row> rows) {
		Set<String> numericFields = new HashSet<String>();
		Row firstRow = rows.get(0);
		List<FieldInfo> fields = new ArrayList<FieldInfo>(rows.size());
		for (String field : firstRow.getFieldNames())
			fields.add(new FieldInfo(field));
		for (Row row : rows) {
			for (FieldInfo fieldInfo : fields) {
				String value = row.get(fieldInfo.name);
				if (fieldInfo.isNumeric && !StringUtilities.isInteger(value))
					fieldInfo.isNumeric = false;
				if (value.length() > fieldInfo.maxLength)
					fieldInfo.maxLength = value.length();
			}
		}
		
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE " + tableName + " (\n");
		for (FieldInfo fieldInfo : fields) {
			sql.append("  " + fieldInfo.toString() + ",\n");
			if (fieldInfo.isNumeric)
				numericFields.add(fieldInfo.name);
		}
		sql.append(");");
		execute(sql.toString());
		return numericFields;
	}
	
	private String columnNameToSqlName(String name) {
		return name.replaceAll(" ", "_").replace("-", "_").replace(",", "_").replaceAll("_+", "_");
	}
	
	private class FieldInfo {
		public String	name;
		public boolean	isNumeric	= true;
		public int		maxLength	= 0;
		
		public FieldInfo(String name) {
			this.name = name;
		}
		
		public String toString() {
			if (dbType == DbType.MYSQL) {
				if (isNumeric)
					return columnNameToSqlName(name) + " int(" + maxLength + ")";
				else if (maxLength > 255)
					return columnNameToSqlName(name) + " text";
				else
					return columnNameToSqlName(name) + " varchar(255)";
			} else if (dbType == DbType.MSSQL) {
				if (isNumeric) {
					if (maxLength < 10)
						return columnNameToSqlName(name) + " int";
					else
						return columnNameToSqlName(name) + " bigint";
				} else if (maxLength > 255)
					return columnNameToSqlName(name) + " varchar(max)";
				else
					return columnNameToSqlName(name) + " varchar(255)";
			} else
				throw new RuntimeException("Create table syntax not specified for type " + dbType);
		}
	}
	
	public void copyTable(String sourceDatabase, String sourceTable, RichConnection targetConnection, String targetDatabase, String targetTable) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE ");
		sql.append(targetDatabase);
		if (targetConnection.dbType == DbType.MSSQL)
			sql.append(".dbo.");
		else
			sql.append(".");
		sql.append(targetTable);
		sql.append("(");
		boolean first = true;
		String query;
		if (dbType == DbType.ORACLE || dbType == DbType.MSSQL)
			query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='" + sourceDatabase + "' AND TABLE_NAME='" + sourceTable
					+ "';";
		else
			// mysql
			query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + sourceDatabase + "' AND TABLE_NAME = '"
					+ sourceTable + "';";
		
		for (Row row : query(query)) {
			if (first)
				first = false;
			else
				sql.append(',');
			sql.append(row.get("COLUMN_NAME"));
			sql.append(' ');
			if (targetConnection.dbType == DbType.ORACLE) {
				if (row.get("DATA_TYPE").equals("bigint"))
					sql.append("number");
				else
					sql.append(row.get("DATA_TYPE"));
			} else {
				sql.append(row.get("DATA_TYPE"));
				if (row.get("DATA_TYPE").equals("varchar"))
					sql.append("(max)");
			}
		}
		sql.append(");");
		targetConnection.execute(sql.toString());
		targetConnection.use(targetDatabase);
		targetConnection.insertIntoTable(query("SELECT * FROM " + sourceDatabase + ".dbo." + sourceTable).iterator(), targetTable, false);
	}
	
	private class DBRowIterator implements Iterator<Row> {
		
		private ResultSet	resultSet;
		
		private boolean		hasNext;
		
		private Set<String>	columnNames	= new HashSet<String>();
		
		public DBRowIterator(String sql) {
			try {
				sql.trim();
				if (sql.endsWith(";"))
					sql = sql.substring(0, sql.length() - 1);
				if (verbose) {
					String abbrSQL = sql.replace('\n', ' ').replace('\t', ' ').trim();
					if (abbrSQL.length() > 100)
						abbrSQL = abbrSQL.substring(0, 100).trim() + "...";
					System.out.println("Executing query: " + abbrSQL);
				}
				long start = System.currentTimeMillis();
				Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				resultSet = statement.executeQuery(sql.toString());
				hasNext = resultSet.next();
				if (verbose)
					outputQueryStats(statement, System.currentTimeMillis() - start);
			} catch (SQLException e) {
				System.err.println(sql.toString());
				System.err.println(e.getMessage());
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
						String value = resultSet.getString(i);
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
}
