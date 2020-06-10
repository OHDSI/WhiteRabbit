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

public class DbType {
	public static DbType	MYSQL		= new DbType("mysql");
	public static DbType	MSSQL		= new DbType("mssql");
	public static DbType	PDW			= new DbType("pdw");
	public static DbType	ORACLE		= new DbType("oracle");
	public static DbType	POSTGRESQL	= new DbType("postgresql");
	public static DbType	MSACCESS	= new DbType("msaccess");
	public static DbType	REDSHIFT	= new DbType("redshift");
	public static DbType	TERADATA	= new DbType("teradata");
	public static DbType	BIGQUERY	= new DbType("bigquery");
	public static DbType	AZURE		= new DbType("azure");

	private enum Type {
		MYSQL, MSSQL, PDW, ORACLE, POSTGRESQL, MSACCESS, REDSHIFT, TERADATA, BIGQUERY, AZURE
	};

	private Type type;

	public DbType(String type) {
		this.type = Type.valueOf(type.toUpperCase());
	}

	public boolean equals(Object other) {
		if (other instanceof DbType && ((DbType) other).type == type)
			return true;
		else
			return false;
	}

	public String getTypeName() {
		return this.type.name();
	}
}
