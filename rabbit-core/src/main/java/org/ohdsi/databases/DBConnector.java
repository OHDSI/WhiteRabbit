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

import oracle.jdbc.pool.OracleDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBConnector {

	public static void main(String[] args) {
	}

	// If dbType.BIGQUERY: domain field has been replaced with  database field
	public static Connection connect(String server, String domain, String user, String password, DbType dbType) {
		if (dbType.equals(DbType.MYSQL))
			return DBConnector.connectToMySQL(server, user, password);
		else if (dbType.equals(DbType.MSSQL) || dbType.equals(DbType.PDW) || dbType.equals(DbType.AZURE))
			return DBConnector.connectToMSSQL(server, domain, user, password);
		else if (dbType.equals(DbType.ORACLE))
			return DBConnector.connectToOracle(server, domain, user, password);
		else if (dbType.equals(DbType.POSTGRESQL))
			return DBConnector.connectToPostgreSQL(server, user, password);
		else if (dbType.equals(DbType.MSACCESS))
			return DBConnector.connectToMsAccess(server, user, password);
		else if (dbType.equals(DbType.REDSHIFT))
			return DBConnector.connectToRedshift(server, user, password);
		else if (dbType.equals(DbType.TERADATA))
			return DBConnector.connectToTeradata(server, user, password);
		else if (dbType.equals(DbType.BIGQUERY))
			return DBConnector.connectToBigQuery(server, domain, user, password);
		else
			return null;
	}

	public static Connection connectToTeradata(String server, String user, String password) {
		try {
			Class.forName("com.teradata.jdbc.TeraDriver");
		} catch(ClassNotFoundException e) {
			throw new RuntimeException("Cannot find JDBC driver. Make sure the terajdbc4.jar and tdgssconfig.jar are in the path");
		}
		String url = "jdbc:teradata://" + server;
		try {
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e1) {
			throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
		}
	}

	public static Connection connectToRedshift(String server, String user, String password) {
		if (!server.contains("/"))
			throw new RuntimeException("For Redshift, database name must be specified in the server field (<host>:<port>/<database>?<options>)");
		try {
			Class.forName("com.amazon.redshift.jdbc42.Driver");
		} catch (ClassNotFoundException e1) {
			throw new RuntimeException("Cannot find JDBC driver. Make sure the file RedshiftJDBCx-x.x.xx.xxxx.jar is in the path");
		}
		String url = "jdbc:redshift://" + server;
		try {
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e1) {
			throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
		}
	}

	public static Connection connectToMsAccess(String server, String user, String password) {
		try {
			Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Cannot find ucanaccess driver. Make sure the file ucanaccess-3.0.3.1.jar is in the path");
		}
		String url = "jdbc:ucanaccess://" + server + ";sysschema=true";
		try {
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot connect to DB server: " + e.getMessage());
		}
	}

	public static Connection connectToPostgreSQL(String server, String user, String password) {
		if (!server.contains("/"))
			throw new RuntimeException("For PostgreSQL, database name must be specified in the server field (<host>/<database>)");
		if (!server.contains(":"))
			server = server.replace("/", ":5432/");
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

		String url = "jdbc:mysql://" + server + "/?useCursorFetch=true&zeroDateTimeBehavior=convertToNull";

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
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e1) {
			throw new RuntimeException("Cannot find JDBC driver. Make sure the file sqljdbc4.jar is in the path");
		}
		String url = "jdbc:sqlserver://" + server;
		if (user == null || user.length() == 0) { // Use Windows integrated security
			url = url + ";integratedSecurity=true";
		}
		try {
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e1) {
			throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
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

	public static Connection connectToBigQuery(String server, String domain, String user, String password) {
		try {
			Class.forName("com.simba.googlebigquery.jdbc42.Driver");
		} catch (ClassNotFoundException e1) {
			throw new RuntimeException("Cannot find Simba GoogleBigQuery JDBC Driver class");
		}
		/* See http://howtodojava.com/regex/java-regex-validate-email.address/ */
		String email_regex = "^[\\w!#$%&'*+/=?`{|}~^-]+(?:\\.[\\w!#$%&'*+/=?`{|}~^-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,6}$";
		Pattern pattern = Pattern.compile(email_regex);
		Matcher matcher = pattern.matcher(user);
		Integer timeout = 3600;
		String url = "";
		if (matcher.matches()) {
			/* use Service Account authentication (less secure - no auditing events to stackdriver) */
			url = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectID=" + server + ";OAuthType=0;OAuthServiceAcctEmail=" + user + ";OAuthPvtKeyPath=" + password + ";DefaultDataset=" + domain + ";Timeout=" + timeout + ";";
		} else {
			/* use application default credentials (more secure - writes auditing events to stackdriver) */
			/* requires user to run: 'gcloud auth application-default login' */
			/* only once on each computer. Writes application key to ~/.config/gcloud/application_default_credentials.json */
			/* See https://cloud.google.com/sdk/gcloud/reference/auth/application-default/ for documentation */
			url = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectID=" + server + ";OAuthType=3;DefaultDataset=" + domain + ";Timeout=" + timeout + ";";
		};
		try {
			return DriverManager.getConnection(url);
		} catch (SQLException e1) {
			throw new RuntimeException("Simba URL failed: Cannot connect to DB server: " + e1.getMessage());
		}
	}
}

