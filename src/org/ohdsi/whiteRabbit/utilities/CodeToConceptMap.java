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

import java.util.HashMap;
import java.util.Map;

import org.ohdsi.utilities.collections.CountingSet;

public class CodeToConceptMap {
	
	private String					name;
	private Map<String, CodeData>	codeToData	= new HashMap<String, CodeData>();
	private CountingSet<String>		codeCounts	= new CountingSet<String>();
	
	public CodeToConceptMap(String name) {
		this.name = name;
	}
	
	public void add(String code, String description, int targetConceptId, String targetCode, String targetDescription) {
		CodeData data = codeToData.get(code);
		if (data == null) {
			data = new CodeData();
			data.description = description;
			codeToData.put(code, data);
			data.targetCodes = new String[] { targetCode };
			data.targetDescriptions = new String[] { targetDescription };
			data.targetConceptIds = new int[] { targetConceptId };
		} else {
			String[] targetCodes = new String[data.targetCodes.length + 1];
			String[] targetDescriptions = new String[data.targetDescriptions.length + 1];
			int[] targetConceptIds = new int[data.targetConceptIds.length + 1];
			System.arraycopy(data.targetCodes, 0, targetCodes, 0, data.targetCodes.length);
			System.arraycopy(data.targetDescriptions, 0, targetDescriptions, 0, data.targetDescriptions.length);
			System.arraycopy(data.targetConceptIds, 0, targetConceptIds, 0, data.targetConceptIds.length);
			targetCodes[targetCodes.length - 1] = targetCode;
			targetDescriptions[targetDescriptions.length - 1] = targetDescription;
			targetConceptIds[targetConceptIds.length - 1] = targetConceptId;
			data.targetCodes = targetCodes;
			data.targetDescriptions = targetDescriptions;
			data.targetConceptIds = targetConceptIds;
		}
	}
	
	public String getName() {
		return name;
	}
	
	public Integer getConceptId(String code) {
		codeCounts.add(code);
		CodeData data = codeToData.get(code);
		if (data == null) {
			return 0;
		} else
			return data.targetConceptIds[0];
	}
	
	public int[] getConceptIds(String code) {
		codeCounts.add(code);
		CodeData data = codeToData.get(code);
		if (data == null)
			return new int[0];
		else
			return data.targetConceptIds;
	}
	
	public CountingSet<String> getCodeCounts() {
		return codeCounts;
	}
	
	public CodeData getCodeData(String code) {
		return codeToData.get(code);
	}
	
	public class CodeData {
		public String	description;
		public int[]	targetConceptIds;
		public String[]	targetCodes;
		public String[]	targetDescriptions;
	}
}
