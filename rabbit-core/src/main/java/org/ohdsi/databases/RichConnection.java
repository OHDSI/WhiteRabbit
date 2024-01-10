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

import java.io.Closeable;
import java.sql.BatchUpdateException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.ohdsi.databases.configuration.DbSettings;
import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.utilities.SimpleCounter;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RichConnection implements Closeable {
	Logger logger = LoggerFactory.getLogger(RichConnection.class);

	public static int INSERT_BATCH_SIZE = 100000;
	private DBConnection connection;
	private boolean verbose = false;
	private DbType dbType;

	public RichConnection(DbSettings dbSettings) {
		this.connection = DBConnector.connect(dbSettings, verbose);
		this.dbType = dbSettings.dbType;
	}

	/**
	 * Execute the given SQL statement.
	 *
	 * @param sql
	 */
	public void execute(String sql) {
		connection.execute(sql, verbose);
	}

	/**
	 * Query the database using the provided SQL statement.
	 *
	 * @param sql
	 * @return
	 */
	public QueryResult query(String sql) {
		return new QueryResult(sql, connection, verbose);
	}

	/**
	 * Switch the database to use.
	 *
	 * @param database
	 */
	public void use(String database) {
		connection.use(database, dbType);
	}

	public List<String> getTableNames(String database) {
		return connection.getTableNames(database);
	}

	public List<FieldInfo> fetchTableStructure(RichConnection connection, String database, String table, ScanParameters scanParameters) {
		return this.connection.fetchTableStructure(this, database, table, scanParameters);
	}

	public QueryResult fetchRowsFromTable(String table, long rowCount, ScanParameters scanParameters) {
		return this.connection.fetchRowsFromTable(table, rowCount, scanParameters);
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
		if (dbType == DbType.SQL_SERVER || dbType == DbType.PDW || dbType == DbType.AZURE)
			qr = query("SELECT COUNT_BIG(*) FROM [" + tableName.replaceAll("\\.", "].[") + "];");
		else if (dbType == DbType.MS_ACCESS)
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
			logger.error(e.getMessage(), e);
		}
	}

	public void setVerbose(boolean verbose) {
		this.connection.setVerbose(verbose);
	}

	public DBConnection getConnection() {
		return connection;
	}


	/**
	 * Inserts the rows into a table in the database.
	 * 
	 * @param iterator
	 * @param table
	 * @param create
	 *            If true, the data format is determined based on the first batch of rows and used to create the table structure.
	 */
	public void insertIntoTable(Iterator<Row> iterator, String table, boolean create) {
		List<Row> batch = new ArrayList<>(INSERT_BATCH_SIZE);

		boolean first = true;
		SimpleCounter counter = new SimpleCounter(1000000, true);
		while (iterator.hasNext()) {
			if (batch.size() == INSERT_BATCH_SIZE) {
				if (first && create) {
					createTable(table, batch);
				}
				insert(table, batch);
				batch.clear();
				first = false;
			}
			batch.add(iterator.next());
			counter.count();
		}
		if (!batch.isEmpty()) {
			if (first && create) {
				createTable(table, batch);
			}
			insert(table, batch);
		}
	}

	boolean isVerbose() {
		return connection.isVerbose();
	}

	private void insert(String tableName, List<Row> rows) {
		List<String> columns;
		columns = rows.get(0).getFieldNames();
        columns.replaceAll(this::columnNameToSqlName);

		StringBuilder sql = new StringBuilder("INSERT INTO " + tableName);
		sql.append(" (").append(StringUtilities.join(columns, ",")).append(")");
		sql.append(" VALUES (?");
		for (int i = 1; i < columns.size(); i++) {
			sql.append(",?");
		}
		sql.append(")");
		try {
			connection.setAutoCommit(false);
			PreparedStatement statement = connection.prepareStatement(sql.toString());
			for (Row row : rows) {
				for (int i = 0; i < columns.size(); i++) {
					String value = row.get(columns.get(i));
					if (value == null) {
						logger.info(row.toString());
					} else if (value.isEmpty()) {
						value = null;
					}
					if (dbType == DbType.POSTGRESQL || dbType == DbType.REDSHIFT) {// PostgreSQL does not allow unspecified types
						statement.setObject(i + 1, value, Types.OTHER);
					}
					else if (dbType == DbType.ORACLE) {
						if (isDate(value)) {
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
			logger.error(e.getMessage(), e);
			if (e instanceof BatchUpdateException) {
				logger.error(e.getNextException().getMessage());
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
				return day >= 1 && day <= 31;
			} catch (Exception e) {
				return false;
			}
		return false;
	}

	private Set<String> createTable(String tableName, List<Row> rows) {
		Set<String> numericFields = new HashSet<>();
		Row firstRow = rows.get(0);
		List<NumericFieldInfo> fields = new ArrayList<>(rows.size());
		for (String field : firstRow.getFieldNames())
			fields.add(new NumericFieldInfo(field));
		for (Row row : rows) {
			for (NumericFieldInfo numericFieldInfo : fields) {
				String value = row.get(numericFieldInfo.name);
				if (numericFieldInfo.isNumeric && !StringUtilities.isInteger(value))
					numericFieldInfo.isNumeric = false;
				if (value.length() > numericFieldInfo.maxLength)
					numericFieldInfo.maxLength = value.length();
			}
		}

		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE ").append(tableName).append(" (\n");
		for (NumericFieldInfo numericFieldInfo : fields) {
			sql.append("  ").append(numericFieldInfo.toString()).append(",\n");
			if (numericFieldInfo.isNumeric)
				numericFields.add(numericFieldInfo.name);
		}
		sql.append(");");
		execute(sql.toString());
		return numericFields;
	}

	private String columnNameToSqlName(String name) {
		return name.replaceAll(" ", "_").replace("-", "_").replace(",", "_").replaceAll("_+", "_");
	}

	private class NumericFieldInfo {
		public String	name;
		public boolean	isNumeric	= true;
		public int		maxLength	= 0;

		public NumericFieldInfo(String name) {
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
			} else if (dbType == DbType.SQL_SERVER || dbType == DbType.PDW || dbType == DbType.AZURE) {
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
}
