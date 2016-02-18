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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RowUtilities {
	public static void sort(List<Row> rows, String... fieldName) {
		if (rows.size() < 2)
			return;
		int[] indexes = new int[fieldName.length];
		for (int i = 0; i < fieldName.length; i++) {
			Integer index = rows.get(0).getfieldName2ColumnIndex().get(fieldName[i]);
			if (index == null)
				throw new RuntimeException("Column '" + fieldName[i] + "' not found");
			indexes[i] = index;
		}
		Collections.sort(rows, new RowComparator(indexes));
	}
	
	private static class RowComparator implements Comparator<Row> {
		private int[]				indexes;
		private StringIdComparator	stringIdComparator	= new StringIdComparator();
		
		public RowComparator(int[] indexes) {
			this.indexes = indexes;
		}
		
		public int compare(Row o1, Row o2) {
			int result = 0;
			int i = 0;
			while (result == 0 && i < indexes.length) {
				String value1 = o1.getCells().get(indexes[i]);
				String value2 = o2.getCells().get(indexes[i]);
				result = stringIdComparator.compare(value1, value2);
				i++;
			}
			return result;
		}
		
	}
}
