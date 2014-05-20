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
package org.ohdsi.whiteRabbit.utilities;

import javax.swing.JOptionPane;

import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.ObjectExchange;

public class ETLUtils {
	public static boolean databaseAlreadyExists(DbSettings dbSettings) {
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		if (connection.getDatabaseNames().contains(dbSettings.database.toLowerCase())) {
			if (ObjectExchange.frame == null) {
				System.out.println("DB already exists. Exiting");
				return true;
			} else {
				String message = "A DB called '" + dbSettings.database + "' alread exists. Do you want to remove it before proceeding?";
				String title = "CDM database already exists";
				int answer = JOptionPane.showConfirmDialog(ObjectExchange.frame, message, title, JOptionPane.YES_NO_OPTION);
				if (answer == JOptionPane.YES_OPTION) {
					StringUtilities.outputWithTime("Dropping database " + dbSettings.database);
					connection.dropDatabaseIfExists(dbSettings.database);
					return false;
				} else {
					System.out.println("DB already exists. Exiting");
					return true;
				}
			}
		} else
			return false;
	}
}
