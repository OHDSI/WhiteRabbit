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
package org.ohdsi.rabbitInAHat;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteCSVFileWithHeader;

/**
 * This class is used as a stand-alone to fetch the structure of the CDM from the server and format it for insertion into Rabbit-In-A-Hat.
 * It is not intended to be used by non-developers.
 * @author MSCHUEMI
 *
 */
public class FetchCDMModelFromServer {
	
	public static void main(String[] args) {
		RichConnection connection = new RichConnection("127.0.0.1/ohdsi", null, "postgres", "F1r3starter", DbType.POSTGRESQL);
		connection.use("cdm5");
		
		WriteCSVFileWithHeader out = new WriteCSVFileWithHeader("c:/temp/CDMV5Model.csv");
		String query = "SELECT table_name,column_name,is_nullable,data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'cdm5';";
		for (Row row : connection.query(query)){
			row.upperCaseFieldNames();
			Row newRow = new Row();
			for (String field : row.getFieldNames())
				newRow.add(field, row.get(field).toUpperCase());
			out.write(newRow);
		}
		out.close();
		
	}
	
}
