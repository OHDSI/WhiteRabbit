/*******************************************************************************
 * Copyright 2016 Observational Health Data Sciences and Informatics
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
package org.ohdsi.whiteRabbit;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.ohdsi.databases.DbType;

public class DbSettings {
	public static int	DATABASE	= 1;
	public static int	CSVFILES	= 2;
	
	public int			dataType;
	public List<String>	tables		= new ArrayList<String>();
	
	// Database settings
	public DbType		dbType;
	public String		user;
	public String		password;
	public String		database;
	public String		server;
	public String		domain;
	
	// CSV file settings
	public char			delimiter	= ',';
	public CSVFormat	csvFormat	= CSVFormat.RFC4180;
}
