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
package org.ohdsi.whiteRabbit.vocabulary;

import java.io.IOException;
import java.util.Iterator;
import java.util.zip.ZipFile;

import javax.swing.JOptionPane;

import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.ObjectExchange;

public class InsertVocabularyInServer {
	
	public static String	VOCABULARY_DB_NAME	= "vocabulary";
	
	private static String[]	tables				= new String[] { "CONCEPT", "CONCEPT_ANCESTOR", "CONCEPT_RELATIONSHIP", "CONCEPT_SYNONYM", "RELATIONSHIP",
			"SOURCE_TO_CONCEPT_MAP", "VOCABULARY" };
	
	public void process(String sourceVocabDataFile, DbSettings dbSettings) {
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(this.getClass());
		connection.setVerbose(false);
		
		if (connection.getDatabaseNames().contains(VOCABULARY_DB_NAME)) {
			if (ObjectExchange.frame == null) {
				System.out.println("Vocab DB already exists. Exiting");
			} else {
				String message = "A DB called '" + VOCABULARY_DB_NAME + "' alread exists. Do you want to remove it before proceding?";
				String title = "Vocabulary database already exists";
				int answer = JOptionPane.showConfirmDialog(ObjectExchange.frame, message, title, JOptionPane.YES_NO_OPTION);
				if (answer == JOptionPane.YES_OPTION) {
					StringUtilities.outputWithTime("Dropping database " + VOCABULARY_DB_NAME);
					connection.dropDatabaseIfExists(VOCABULARY_DB_NAME);
				} else {
					System.out.println("Vocab DB already exists. Exiting");
					return;
				}
			}
		}
		
		StringUtilities.outputWithTime("Creating vocabulary data structure");
		connection.executeResource("CreateVocabStructure.sql");
		connection.use("vocabulary");
		try {
			ZipFile zipFile = new ZipFile(sourceVocabDataFile);
			for (String table : tables) {
				StringUtilities.outputWithTime("Inserting data for table " + table);
				Iterator<Row> iterator = new ReadCSVFileWithHeader(zipFile.getInputStream(zipFile.getEntry(table + ".csv"))).iterator();
				connection.insertIntoTable(iterator, table, false);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		StringUtilities.outputWithTime("Creating vocabulary indices");
		connection.executeResource("CreateVocabIndices.sql");
		StringUtilities.outputWithTime("Finished inserting vocabulary in server");
	}
	
}
