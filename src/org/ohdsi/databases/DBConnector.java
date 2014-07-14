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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import oracle.jdbc.pool.OracleDataSource;

import org.ohdsi.utilities.files.Row;

public class DBConnector {

	public static void main(String[] args) {

		// System.out.println(System.getProperty("java.version"));
		// System.out.println(System.getProperty("java.vendor"));
		// System.out.println(System.getProperty("java.vm.name"));
		// for (Map.Entry e : System.getProperties().entrySet()) {
		// if (((String) e.getKey()).startsWith("java")) {
		// System.out.println(e);
		// }
		// }
		// Connection connection = DBConnector.connect("xe", null, "system", "F1r3starter", DbType.ORACLE);

		// RichConnection richConnection = new RichConnection("RNDUSRDHIT06.jnj.com", "eu", "mschuemi", "Muad'Dib", DbType.MSSQL);
		// RichConnection richConnection = new RichConnection("localhost/test", null, "postgres", "F1r3starter", DbType.POSTGRESQL);

		RichConnection richConnection = new RichConnection("127.0.0.1:1521/xe", null, "system", "F1r3starter", DbType.ORACLE);

		richConnection.setVerbose(true);

		// richConnection.execute("CREATE USER test IDENTIFIED BY \"test\"");
		richConnection.use("test");
		// for (Row row : richConnection.query("SELECT count(*) FROM test_table"))
		// System.out.println(row.toString());
		//
		// richConnection.execute("TRUNCATE TABLE test_table");

		for (Row row : richConnection.query("SELECT * FROM test_table WHERE rownum < 2000"))
			System.out.println(row.toString());
		// richConnection.use("test");
		// richConnection.execute("GRANT UNLIMITED TABLESPACE TO test");
		// richConnection.execute("ALTER SESSION SET current_schema = test");

		// richConnection.execute("CREATE TABLE test_table (key integer)");
		// List<Row> rows = new ArrayList<Row>();
		// for (int i = 1; i < 10000000; i++) {
		// Row row = new Row();
		// row.add("key", i);
		// rows.add(row);
		// }
		//
		// richConnection.insertIntoTable(rows.iterator(), "test_table", false);

	}

	public static Connection connect(String server, String domain, String user, String password, DbType dbType) {
		if (dbType.equals(DbType.MYSQL))
			return DBConnector.connectToMySQL(server, user, password);
		else if (dbType.equals(DbType.MSSQL))
			return DBConnector.connectToMSSQL(server, domain, user, password);
		else if (dbType.equals(DbType.ORACLE))
			return DBConnector.connectToOracle(server, domain, user, password);
		else if (dbType.equals(DbType.POSTGRESQL))
			return DBConnector.connectToPostgreSQL(server, user, password);
		else
			return null;
	}

	public static Connection connectToPostgreSQL(String server, String user, String password) {
		if (!server.contains("/"))
			throw new RuntimeException("For PostgreSQL, database name must be specified in the server field (<host>/<database>)");
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e1) {
			throw new RuntimeException("Cannot find JDBC driver. Make sure the file postgresql-x.x-xxxx.jdbcx.jar is in the path");
		}
		String url = "jdbc:postgresql://" + server;
		try {
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e1) {
			throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
		}
	}

	public static Connection connectToMySQL(String server, String user, String password) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e1) {
			throw new RuntimeException("Cannot find JDBC driver. Make sure the file mysql-connector-java-x.x.xx-bin.jar is in the path");
		}

		String url = "jdbc:mysql://" + server + ":3306/?useCursorFetch=true";

		try {
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e1) {
			throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
		}
	}

	public static Connection connectToODBC(String server, String user, String password) {
		try {
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
		} catch (ClassNotFoundException e1) {
			throw new RuntimeException("Cannot find ODBC driver");
		}

		String url = "jdbc:odbc:" + server;

		try {
			Connection connection = DriverManager.getConnection(url, user, password);

			return connection;
		} catch (SQLException e1) {
			throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
		}
	}

	/*
	 * public static Connection connectToMSSQL(String server, String domain, String user, String password) { try {
	 * Class.forName("net.sourceforge.jtds.jdbc.Driver");
	 * 
	 * } catch (ClassNotFoundException e1) { throw new RuntimeException("Cannot find JDBC driver. Make sure the file sqljdbc4.jar is in the path"); }
	 * 
	 * String url = "jdbc:jtds:sqlserver://"+server+(domain.length()==0?"":";domain="+domain);
	 * 
	 * try { return DriverManager.getConnection(url,user, password); } catch (SQLException e1) { throw new RuntimeException("Cannot connect to DB server: " +
	 * e1.getMessage()); } }
	 */
	public static Connection connectToMSSQL(String server, String domain, String user, String password) {
		if (user == null || user.length() == 0) { // Use Windows integrated security
			try {
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			} catch (ClassNotFoundException e1) {
				throw new RuntimeException("Cannot find JDBC driver. Make sure the file sqljdbc4.jar is in the path");
			}
			String url = "jdbc:sqlserver://" + server + ";integratedSecurity=true";

			try {
				return DriverManager.getConnection(url, user, password);
			} catch (SQLException e1) {
				throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
			}
		} else { // Do not use Windows integrated security
			try {
				Class.forName("net.sourceforge.jtds.jdbc.Driver");
			} catch (ClassNotFoundException e1) {
				throw new RuntimeException("Cannot find JDBC driver. Make sure the file jtds-1.3.0.jar is in the path");
			}

			String url = "jdbc:jtds:sqlserver://" + server + ";ssl=required" + ((domain == null || domain.length() == 0) ? "" : ";domain=" + domain);

			try {
				return DriverManager.getConnection(url, user, password);
			} catch (SQLException e1) {
				throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
			}
		}

	}

	public static Connection connectToOracle(String server, String domain, String user, String password) {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Class not found exception: " + e.getMessage());
		}
		// First try OCI driver:
		String error = null;
		try {
			OracleDataSource ods;
			ods = new OracleDataSource();
			ods.setURL("jdbc:oracle:oci8:@" + server);
			ods.setUser(user);
			ods.setPassword(password);
			return ods.getConnection();
		} catch (UnsatisfiedLinkError e) {
			error = e.getMessage();
		} catch (SQLException e) {
			error = e.getMessage();
		}
		// If fails, try THIN driver:
		if (error != null)
			try {
				String host = "127.0.0.1";
				String sid = server;
				String port = "1521";
				if (server.contains("/")) {
					host = server.split("/")[0];
					if (host.contains(":")) {
						port = host.split(":")[1];
						host = host.split(":")[0];
					}
					sid = server.split("/")[1];
				}
				OracleDataSource ods;
				ods = new OracleDataSource();
				ods.setURL("jdbc:oracle:thin:@" + host + ":" + port + ":" + sid);
				ods.setUser(user);
				ods.setPassword(password);
				return ods.getConnection();
			} catch (SQLException e) {
				throw new RuntimeException("Cannot connect to DB server:\n- When using OCI: " + error + "\n- When using THIN: " + e.getMessage());
			}
		return null;
	}
}
