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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.IntegerComparator;
import org.ohdsi.utilities.collections.LongComparator;
import org.ohdsi.utilities.files.Row;

/**
 * Creates drug_eras for a single person
 * 
 * @author MSCHUEMI
 */
public class ConditionEraGenerator {
	
	public static int					PERSISTENCE_WINDOW	= 30;
	public static int					TYPE_CONCEPT_ID		= 38000247;
	public static int					CONDITION_ERA_ID	= 1;
	private List<ConditionOccurence>	conditions			= new ArrayList<ConditionOccurence>();
	
	public void addCondition(int personId, long startDate, long endDate, int conceptId) {
		if (startDate != StringUtilities.MISSING_DATE)
			conditions.add(new ConditionOccurence(personId, startDate, endDate, conceptId));
	}
	
	public List<Row> generateRows() {
		if (conditions.size() == 0)
			return Collections.emptyList();
		
		List<Row> rows = new ArrayList<Row>();
		Collections.sort(conditions, new Comparator<ConditionOccurence>() {
			
			@Override
			public int compare(ConditionOccurence o1, ConditionOccurence o2) {
				int result = IntegerComparator.compare(o1.personId, o2.personId);
				if (result == 0)
					result = LongComparator.compare(o1.startDate, o2.startDate);
				return result;
			}
		});
		
		int oldPersonId = Integer.MIN_VALUE;
		List<ConditionOccurence> personExposures = new ArrayList<ConditionOccurence>();
		for (ConditionOccurence exposure : conditions)
			if (exposure.personId == oldPersonId)
				personExposures.add(exposure);
			else {
				processPerson(personExposures, rows);
				personExposures.clear();
				oldPersonId = exposure.personId;
			}
		processPerson(personExposures, rows);
		return rows;
	}
	
	private void processPerson(List<ConditionOccurence> personExposures, List<Row> rows) {
		Map<Integer, ConditionOccurence> conceptIdToLastExposure = new HashMap<Integer, ConditionEraGenerator.ConditionOccurence>();
		for (ConditionOccurence condition : personExposures) {
			ConditionOccurence lastCondition = conceptIdToLastExposure.get(condition.conceptId);
			if (lastCondition == null)
				conceptIdToLastExposure.put(condition.conceptId, condition);
			else if (lastCondition.endDate + PERSISTENCE_WINDOW >= condition.startDate) {
				lastCondition.endDate = condition.endDate;
				lastCondition.conditionCount++;
			} else {
				Row row = lastCondition.toRow();
				if (row != null)
					rows.add(row);
				conceptIdToLastExposure.put(condition.conceptId, condition);
			}
		}
		for (ConditionOccurence exposure : conceptIdToLastExposure.values()) {
			Row row = exposure.toRow();
			if (row != null)
				rows.add(row);
		}
	}
	
	private class ConditionOccurence {
		public int	personId;
		public long	startDate;
		public long	endDate;
		public int	conceptId;
		public int	conditionCount	= 1;
		
		public ConditionOccurence(int personId, long startDate, long endDate, int conceptId) {
			this.personId = personId;
			this.startDate = startDate;
			this.endDate = endDate;
			this.conceptId = conceptId;
		}
		
		public Row toRow() {
			Row row = new Row();
			row.add("condition_era_id", CONDITION_ERA_ID++);
			row.add("person_id", personId);
			row.add("condition_concept_id", conceptId);
			if (startDate == StringUtilities.MISSING_DATE)
				return null;
			row.add("condition_era_start_date", StringUtilities.daysToDatabaseDateString(startDate));
			if (endDate == StringUtilities.MISSING_DATE)
				row.add("condition_era_end_date", StringUtilities.daysToDatabaseDateString(startDate));
			else
				row.add("condition_era_end_date", StringUtilities.daysToDatabaseDateString(endDate));
			row.add("condition_type_concept_id", TYPE_CONCEPT_ID);
			row.add("condition_occurrence_count", conditionCount);
			return row;
		}
		
	}
}
