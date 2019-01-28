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
package org.ohdsi.utilities.files;

import java.util.HashMap;
import java.util.Map;

public class IniFile {
	private Map<String, String> settings = new HashMap<String, String>();

	public IniFile(String filename){
		for (String line : new ReadTextFile(filename)){
			int indexOfHash = line.lastIndexOf('#');
			if (indexOfHash != -1)
				line = line.substring(0,indexOfHash);

			int	indexOfEqualsSigns = line.indexOf('=');

			if (indexOfEqualsSigns != -1)
				settings.put(line.substring(0,indexOfEqualsSigns).trim().toLowerCase(), line.substring(indexOfEqualsSigns+1).trim());	
		}
	}

	public String get(String fieldName){
		String value = settings.get(fieldName.toLowerCase());
		if (value == null)
			return "";
		else
			return value;
	}
}
