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

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteCSVFileWithHeader;


public class FetchVocabularyFromServer {
	
	public static String server = "RNDUSRDHIT06";
	public static String database = "OMOP_Vocabulary";
	public static String outputFolder = "S:/Data/OMOP Standard Vocabulary V4/FromJnJServer/";
	public static String[] tables = new String[]{"CONCEPT","CONCEPT_ANCESTOR","CONCEPT_RELATIONSHIP","CONCEPT_SYNONYM","RELATIONSHIP","SOURCE_TO_CONCEPT_MAP","VOCABULARY"};
	
	public static void main(String[] args) {
		RichConnection connection = new RichConnection(server, null, null, null,  DbType.MSSQL);
		connection.use(database);
		
		for (String table : tables){
			System.out.println("Writing table " + table);
			
			WriteCSVFileWithHeader out = new WriteCSVFileWithHeader(outputFolder+table+".csv");
			for (Row row : connection.query("SELECT * FROM " + table))
				out.write(row);
			out.close();
		}
		
		
	}
	
}
