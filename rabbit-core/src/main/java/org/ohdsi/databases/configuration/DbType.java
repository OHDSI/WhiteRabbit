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
package org.ohdsi.databases.configuration;

import org.apache.commons.lang.StringUtils;
import org.ohdsi.databases.DatabricksHandler;
import org.ohdsi.databases.JdbcStorageHandler;
import org.ohdsi.databases.SnowflakeHandler;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum DbType {
	/*
	 * Please note: the names and strings and the Type enum below must match when String.toUpperCase().replace(" ", "_")
	 * is applied (see constructor and the normalizedName() method). This is enforced when the enum values are constructed,
	 * and a violation of this rule will result in a ScanConfigurationException being thrown.
	 */
	DELIMITED_TEXT_FILES("Delimited text files", null),
	MYSQL("MySQL", "com.mysql.cj.jdbc.Driver"),
	ORACLE("Oracle", "oracle.jdbc.driver.OracleDriver"),
	SQL_SERVER("SQL Server", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
	POSTGRESQL("PostgreSQL", "org.postgresql.Driver"),
	MS_ACCESS("MS Access", "net.ucanaccess.jdbc.UcanaccessDriver"),
	PDW("PDW", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
	REDSHIFT("Redshift", "com.amazon.redshift.jdbc42.Driver"),
	TERADATA("Teradata", "com.teradata.jdbc.TeraDriver", null, false),
	BIGQUERY("BigQuery", "com.simba.googlebigquery.jdbc42.Driver", null, false),	// license does not allow inclusion with the distribution
	AZURE("Azure", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
	SNOWFLAKE("Snowflake", "net.snowflake.client.jdbc.SnowflakeDriver", SnowflakeHandler.INSTANCE),
	SAS7BDAT("Sas7bdat", null),
	DATABRICKS("Databricks", DatabricksHandler.DATABRICKS_JDBC_CLASSNAME, DatabricksHandler.INSTANCE);

	private final String label;
	private final String driverName;
	private final JdbcStorageHandler implementingClass;
	private final boolean driverIncluded;

	DbType(String type, String driverName) {
		this(type, driverName, null, true);
	}

	DbType(String label, String driverName, JdbcStorageHandler implementingClass) {
		this(label, driverName, implementingClass, true);
	}

	DbType(String label, String driverName, JdbcStorageHandler implementingClass, boolean included) {
		this.label = label;
		this.driverName = driverName;
		this.implementingClass = implementingClass;
		this.driverIncluded = included;
		if (!this.name().equals(normalizedName(label))) {
			throw new ScanConfigurationException(String.format(
					"%s: the normalized value of label '%s' (%s) must match the name of the enum constant (%s)",
					DbType.class.getName(),
					label,
					normalizedName(label),
					this.name()
			));
		}
	}

	public boolean equalsDbType(DbType other) {
		return (other != null && other.equals(this));
	}

	public boolean supportsStorageHandler() {
		return this.implementingClass != null;
	}

	public JdbcStorageHandler getStorageHandler() throws ScanConfigurationException {
		if (this.supportsStorageHandler()) {
			return this.implementingClass;
		} else {
			throw new ScanConfigurationException(String.format("Class %s does not implement interface %s",
					this.implementingClass.getClass().getName(),
					JdbcStorageHandler.class.getName()));
		}
	}

	public static DbType getDbType(String name) {
		return Enum.valueOf(DbType.class, normalizedName(name));
	}

	/**
	 * Returns the list of supported database in the order that they should appear in the GUI.
	 *
	 * @return Array of labels for the supported database, intended for use in a selector (like a Swing JComboBox)
	 */
	public static String[] pickList() {
		return Stream.of(DELIMITED_TEXT_FILES, SAS7BDAT, MYSQL, ORACLE, SQL_SERVER, POSTGRESQL, MS_ACCESS, PDW, REDSHIFT, TERADATA, BIGQUERY, AZURE, SNOWFLAKE, DATABRICKS)
				.map(DbType::label).toArray(String[]::new);
	}

	public static List<String> driverNames() {
		// return a list of unique names, without null values
		return Stream.of(values()).filter(v -> StringUtils.isNotEmpty(v.driverName)).map(d -> d.driverName).distinct().collect(Collectors.toList());
	}
	public static List<String> includedDriverNames() {
		// return a list of unique names for drivers that are included with the distribution, without null values
		return Stream.of(values()).filter(v -> StringUtils.isNotEmpty(v.driverName) && v.driverIncluded).map(d -> d.driverName).distinct().collect(Collectors.toList());
	}

	public static List<String> excludedDriverNames() {
		// return a list of unique names for drivers that are excluded from the distribution, without null values
		return Stream.of(values()).filter(v -> StringUtils.isNotEmpty(v.driverName) && !v.driverIncluded).map(d -> d.driverName).distinct().collect(Collectors.toList());
	}

	public String label() {
		return this.label;
	}

	public String driverName() {
		return this.driverName;
	}

	private static String normalizedName(String name) {
		return name.toUpperCase().replace(" ", "_");
	}
}
