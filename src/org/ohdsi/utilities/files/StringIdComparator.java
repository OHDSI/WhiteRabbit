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
package org.ohdsi.utilities.files;

import java.util.Comparator;

/**
 * Comparator for strings. If both strings are numbers, they are compared as numbers, else as strings
 * @author MSCHUEMI
 *
 */
public class StringIdComparator implements Comparator<String> {
	
	@Override
	public int compare(String value1, String value2) {
		if (isNumber(value1)) {
			if (isNumber(value2))
				return efficientLongCompare(value1, value2);
			else
				return 1;
		} else {
			if (isNumber(value2))
				return -1;
			else
				return value1.compareTo(value2);
		}
	}
	
	private int efficientLongCompare(String value1, String value2) {
		if (value1.length() > value2.length())
			return 1;
		else if (value1.length() < value2.length())
			return -1;
		else
			return value1.compareTo(value2);
	}
	
	private boolean isNumber(String string) {
		if (string.length() == 0)
			return false;
		for (int i = 0; i < string.length(); i++)
			if (!Character.isDigit(string.charAt(i)))
				return false;
		return true;
	}
	
}
