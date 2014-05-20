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
package org.ohdsi.whiteRabbit.etls.ars;

import java.util.HashMap;
import java.util.Map;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteCSVFileWithHeader;

public class AddExtraInfoToAtcToRxNormMap {
	
	public static void main(String[] args) {
		RichConnection connection = new RichConnection("RNDUSRDHIT06", null, null, null, DbType.MSSQL);
		connection.setContext(AddExtraInfoToAtcToRxNormMap.class);
		connection.use("OMOP_Vocabulary");
		
		Map<String, Integer> atcToConceptId = new HashMap<String, Integer>();
		Map<Integer, String> conceptIdToRxNormCode = new HashMap<Integer, String>();
		
		String query = "SELECT concept_id,concept_name,concept_code FROM concept WHERE VOCABULARY_ID = 21";
		for (Row row : connection.query(query))
			atcToConceptId.put(row.get("CONCEPT_CODE"), row.getInt("CONCEPT_ID"));
		
		query = "SELECT concept_id,concept_name,concept_code FROM concept WHERE VOCABULARY_ID = 8";
		for (Row row : connection.query(query))
			conceptIdToRxNormCode.put(row.getInt("CONCEPT_ID"),row.get("CONCEPT_CODE"));
		
		WriteCSVFileWithHeader out = new WriteCSVFileWithHeader("c:/temp/AtcToRxNormIngredient.csv");
		for (Row row : new ReadCSVFileWithHeader(AddExtraInfoToAtcToRxNormMap.class.getResourceAsStream("ATCToConceptID.csv"))){
			Integer atcConceptId = atcToConceptId.get(row.get("ATC"));
			if (atcConceptId == null)
				atcConceptId = 0;
			String rxNormCode = conceptIdToRxNormCode.get(row.getInt("ConceptID"));
			if (rxNormCode == null)
				rxNormCode = "";
			row.add("atc_concept_id", atcConceptId);
			row.add("rxnorm_code", rxNormCode);
			out.write(row);
		}
		out.close();
	}
	
}
