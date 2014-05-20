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
public class DrugEraGenerator {
	
	public static int			PERSISTENCE_WINDOW	= 30;
	public static int			TYPE_CONCEPT_ID		= 38000182;
	public static int			DRUG_ERA_ID			= 1;
	private List<DrugExposure>	exposures			= new ArrayList<DrugExposure>();
	
	public void addExposure(int personId, long startDate, long endDate, int conceptId) {
		if (startDate != StringUtilities.MISSING_DATE)
			exposures.add(new DrugExposure(personId, startDate, endDate, conceptId));
	}
	
	public List<Row> generateRows() {
		if (exposures.size() == 0)
			return Collections.emptyList();
		
		List<Row> rows = new ArrayList<Row>();
		Collections.sort(exposures, new Comparator<DrugExposure>() {
			
			@Override
			public int compare(DrugExposure o1, DrugExposure o2) {
				int result = IntegerComparator.compare(o1.personId, o2.personId);
				if (result == 0)
					result = LongComparator.compare(o1.startDate, o2.startDate);
				return result;
			}
		});
		
		int oldPersonId = Integer.MIN_VALUE;
		List<DrugExposure> personExposures = new ArrayList<DrugExposure>();
		for (DrugExposure exposure : exposures)
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
	
	private void processPerson(List<DrugExposure> personExposures, List<Row> rows) {
		Map<Integer, DrugExposure> conceptIdToLastExposure = new HashMap<Integer, DrugExposure>();
		for (DrugExposure exposure : personExposures) {
			DrugExposure lastExposure = conceptIdToLastExposure.get(exposure.conceptId);
			if (lastExposure == null)
				conceptIdToLastExposure.put(exposure.conceptId, exposure);
			else if (lastExposure.endDate + PERSISTENCE_WINDOW >= exposure.startDate) {
				lastExposure.endDate = exposure.endDate;
				lastExposure.exposureCount++;
			} else {
				rows.add(lastExposure.toRow());
				conceptIdToLastExposure.put(exposure.conceptId, exposure);
			}
		}
		for (DrugExposure exposure : conceptIdToLastExposure.values())
			rows.add(exposure.toRow());
	}
	
	private class DrugExposure {
		public int	personId;
		public long	startDate;
		public long	endDate;
		public int	conceptId;
		public int	exposureCount	= 1;
		
		public DrugExposure(int personId, long startDate, long endDate, int conceptId) {
			this.personId = personId;
			this.startDate = startDate;
			this.endDate = endDate;
			this.conceptId = conceptId;
		}
		
		public Row toRow() {
			Row row = new Row();
			row.add("drug_era_id", DRUG_ERA_ID++);
			row.add("person_id", personId);
			row.add("drug_concept_id", conceptId);
			if (startDate == StringUtilities.MISSING_DATE)
				row.add("drug_era_start_date", "");
			else
				row.add("drug_era_start_date", StringUtilities.daysToDatabaseDateString(startDate));
			if (endDate == StringUtilities.MISSING_DATE)
				row.add("drug_era_end_date", "");
			else
				row.add("drug_era_end_date", StringUtilities.daysToDatabaseDateString(endDate));
			row.add("drug_type_concept_id", TYPE_CONCEPT_ID);
			row.add("drug_exposure_count", exposureCount);
			return row;
		}
	}
}
