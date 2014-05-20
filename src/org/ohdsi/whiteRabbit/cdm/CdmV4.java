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
package org.ohdsi.whiteRabbit.cdm;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.whiteRabbit.DbSettings;

public class CdmV4 {
	
	public static void createStructure(DbSettings dbSettings) {
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(CdmV4.class);
		connection.setVerbose(false);
		
		StringUtilities.outputWithTime("Creating CDM V4 data structure");
		if (dbSettings.dbType == DbType.MYSQL) {
			connection.execute("CREATE DATABASE IF NOT EXISTS `" + dbSettings.database + "`");
			connection.use(dbSettings.database);
			connection.executeResource("CreateCDMStructureMySQL.sql");
		} else if (dbSettings.dbType == DbType.MSSQL) {
			//connection.execute("CREATE DATABASE IF NOT EXISTS \"" + dbSettings.database + "\"");
			connection.use(dbSettings.database);
			connection.executeResource("CreateCDMStructureSQLServer.sql");
		}

	}
	
	public static void createIndices(DbSettings dbSettings) {
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(CdmV4.class);
		connection.setVerbose(false);
		
		StringUtilities.outputWithTime("Creating CDM V4 indices");
		connection.use(dbSettings.database);
		connection.executeResource("CreateCDMIndices.sql");
	}
}
