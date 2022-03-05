/*******************************************************************************
 * Copyright 2019 Observational Health Data Sciences and Informatics
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

import org.ohdsi.utilities.SimpleCounter;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.Row;

import java.io.Closeable;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;

public class RichConnection implements Closeable {
	public static int				INSERT_BATCH_SIZE	= 100000;
	private Connection				connection;
	private boolean					verbose				= false;
	private static DecimalFormat	decimalFormat		= new DecimalFormat("#.#");
	private DbType					dbType;

	public RichConnection(String server, String domain, String user, String password, DbType dbType) {
		this.connection = DBConnector.connect(server, domain, user, password, dbType);
		this.dbType = dbType;
	}

	/**
	 * Execute the given SQL statement.
	 * 
	 * @param sql
	 */
	public void execute(String sql) {
		Statement statement = null;
		try {
			if (sql.length() == 0)
				return;

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
		} catch (SQLException e) {
			System.err.println(sql);
			e.printStackTrace();
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					System.err.println(e.getMessage());
				}
			}
		}
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
		if (database == null || dbType == DbType.MSACCESS || dbType == DbType.BIGQUERY || dbType == DbType.AZURE) {
			return;
		}

		if (dbType == DbType.ORACLE) {
			execute("ALTER SESSION SET current_schema = " + database);
		} else if (dbType == DbType.POSTGRESQL || dbType == DbType.REDSHIFT) {
			execute("SET search_path TO " + database);
		} else if (dbType == DbType.TERADATA) {
			execute("database " + database);
		} else {
			execute("USE " + database);
		}
	}

	/**
	** Postgres Sql - Case insensitive
	**/
	public List<String> getTableNames(String database) {
		List<String> names = new ArrayList<>();
		String query = null;
		if (dbType == DbType.MYSQL) {
			query = "SHOW TABLES IN " + database;
		} else if (dbType == DbType.MSSQL || dbType == DbType.PDW || dbType == DbType.AZURE) {
			query = "SELECT CONCAT(schemas.name, '.', tables_views.name) FROM " +
					"(SELECT schema_id, name FROM %1$s.sys.tables UNION ALL SELECT schema_id, name FROM %1$s.sys.views) tables_views " +
					"INNER JOIN %1$s.sys.schemas ON tables_views.schema_id = schemas.schema_id " +
					"ORDER BY schemas.name, tables_views.name";
			query = String.format(query, database);
			System.out.println(query);
		} else if (dbType == DbType.ORACLE) {
			query = "SELECT table_name FROM " +
					"(SELECT table_name, owner FROM all_tables UNION ALL SELECT view_name, owner FROM all_views) tables_views " +
					"WHERE owner='" + database.toUpperCase() + "'";
		} else if (dbType == DbType.POSTGRESQL || dbType == DbType.REDSHIFT) {
			query = "SELECT table_name FROM information_schema.tables WHERE LOWER(table_schema) = '" + database.toLowerCase() + "' ORDER BY table_name";
		} else if (dbType == DbType.MSACCESS) {
			query = "SELECT Name FROM sys.MSysObjects WHERE (Type=1 OR Type=5) AND Flags=0;";
		} else if (dbType == DbType.TERADATA) {
			query = "SELECT TableName from dbc.tables WHERE tablekind IN ('T','V') and databasename='" + database + "'";
		} else if (dbType == DbType.BIGQUERY) {
			query = "SELECT table_name from " + database + ".INFORMATION_SCHEMA.TABLES ORDER BY table_name;";
		}

		for (Row row : query(query))
			names.add(row.get(row.getFieldNames().get(0)));
		return names;
	}

	public ResultSet getMsAccessFieldNames(String table) {
		if (dbType == DbType.MSACCESS) {
			try {
				DatabaseMetaData metadata = connection.getMetaData();
				return metadata.getColumns(null, null, table, null);
			} catch (SQLException e) {
				throw new RuntimeException(e.getMessage());
			}
		} else
			throw new RuntimeException("DB is not of type MS Access");
	}

	/**
	 * Returns the row count of the specified table.
	 * 
	 * @param tableName
	 * @return
	 */
	public long getTableSize(String tableName) {
		QueryResult qr;
		long returnVal;
		if (dbType == DbType.MSSQL || dbType == DbType.PDW || dbType == DbType.AZURE)
			qr = query("SELECT COUNT_BIG(*) FROM [" + tableName.replaceAll("\\.", "].[") + "];");
		else if (dbType == DbType.MSACCESS)
			qr = query("SELECT COUNT(*) FROM [" + tableName + "];");
		else
			qr = query("SELECT COUNT(*) FROM " + tableName + ";");
		try {
			returnVal = Long.parseLong(qr.iterator().next().getCells().get(0));
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (qr != null) {
				qr.close();
			}
		}
		return returnVal;
	}

	/**
	 * Close the connection to the database.
	 */
	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public class QueryResult implements Iterable<Row> {
		private String				sql;

		private List<DBRowIterator>	iterators	= new ArrayList<>();

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
	 * Inserts the rows into a table in the database.
	 * 
	 * @param iterator
	 * @param table
	 * @param create
	 *            If true, the data format is determined based on the first batch of rows and used to create the table structure.
	 */
	public void insertIntoTable(Iterator<Row> iterator, String table, boolean create) throws SQLException {
		List<Row> batch = new ArrayList<>(INSERT_BATCH_SIZE);

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

	private void insert(String tableName, List<Row> rows) throws SQLException {
		List<String> columns;
		columns = rows.get(0).getFieldNames();
		for (int i = 0; i < columns.size(); i++)
			columns.set(i, columnNameToSqlName(columns.get(i)));

		StringBuilder sql = new StringBuilder("INSERT INTO " + tableName);
		sql.append(" (").append(StringUtilities.join(columns, ",")).append(")");
		sql.append(" VALUES (?");
		for (int i = 1; i < columns.size(); i++)
			sql.append(",?");
		sql.append(")");
		try {
			connection.setAutoCommit(false);
			PreparedStatement statement = connection.prepareStatement(sql.toString());
			for (Row row : rows) {
				for (int i = 0; i < columns.size(); i++) {
					String value = row.get(columns.get(i));
					if (value == null)
						System.out.println(row.toString());
					else if (value.length() == 0)
						value = null;
					// System.out.println(value);
					if (dbType == DbType.POSTGRESQL || dbType == DbType.REDSHIFT) // PostgreSQL does not allow unspecified types
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
				System.err.println(e.getNextException().getMessage());
			}
			throw e;
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
				return day >= 1 && day <= 31;
			} catch (Exception e) {
				return false;
			}
		return false;
	}

	private Set<String> createTable(String tableName, List<Row> rows) {
		Set<String> numericFields = new HashSet<>();
		Row firstRow = rows.get(0);
		List<FieldInfo> fields = new ArrayList<>(rows.size());
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
		sql.append("CREATE TABLE ").append(tableName).append(" (\n");
		for (FieldInfo fieldInfo : fields) {
			sql.append("  ").append(fieldInfo.toString()).append(",\n");
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
			} else if (dbType == DbType.MSSQL || dbType == DbType.PDW || dbType == DbType.AZURE) {
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

	private class DBRowIterator implements Iterator<Row> {

		private ResultSet	resultSet;

		private boolean		hasNext;

		private Set<String>	columnNames	= new HashSet<>();

		public DBRowIterator(String sql) {
			Statement statement;
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
				statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				resultSet = statement.executeQuery(sql);
				hasNext = resultSet.next();
				if (verbose)
					outputQueryStats(statement, System.currentTimeMillis() - start);
			} catch (SQLException e) {
				System.err.println(sql);
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
}
