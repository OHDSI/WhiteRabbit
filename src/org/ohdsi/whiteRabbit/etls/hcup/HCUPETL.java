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
package org.ohdsi.whiteRabbit.etls.hcup;

import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.OneToManyList;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.EtlReport;
import org.ohdsi.whiteRabbit.cdm.CDMV4NullableChecker;
import org.ohdsi.whiteRabbit.cdm.CdmV4;
import org.ohdsi.whiteRabbit.utilities.CodeToConceptMap;
import org.ohdsi.whiteRabbit.utilities.ETLUtils;
import org.ohdsi.whiteRabbit.utilities.QCSampleConstructor;

public class HCUPETL {
	
	public static int					BATCH_SIZE					= 10000;
	public static String[]				diagnoseFields				= new String[] { "DX1", "DX2", "DX3", "DX4", "DX5", "DX6", "DX7", "DX8", "DX9", "DX10",
			"DX11", "DX12", "DX13", "DX14", "DX15", "DX16", "DX17", "DX18", "DX19", "DX20", "DX21", "DX22", "DX23", "DX24", "DX25", "ECODE1", "ECODE2" };
	public static String[]				procedureFields				= new String[] { "PR1", "PR2", "PR3", "PR4", "PR5", "PR6", "PR7", "PR8", "PR9", "PR10",
			"PR11", "PR12", "PR13", "PR14", "PR15"					};
	public static String[]				procedureDayFields			= new String[] { "PRDAY1", "PRDAY2", "PRDAY3", "PRDAY4", "PRDAY5", "PRDAY6", "PRDAY7",
			"PRDAY8", "PRDAY9", "PRDAY10", "PRDAY11", "PRDAY12", "PRDAY13", "PRDAY14", "PRDAY15" };
	public static int[]					diagnoseFieldConceptIds		= new int[] { 38000184, 38000185, 38000186, 38000187, 38000188, 38000189, 38000190,
			38000191, 38000192, 38000193, 38000194, 38000195, 38000196, 38000197, 38000198, 38000198, 380001980, 38000198, 38000198, 38000198, 38000198,
			38000198, 38000198, 38000198, 38000198, 38000184, 38000185			};
	
	public static int[]					procedureFieldConceptIds	= new int[] { 38000251, 38000252, 38000253, 38000254, 38000255, 38000256, 38000257,
			38000258, 38000259, 38000260, 38000261, 38000262, 38000263, 38000264, 38000265 };
	
	private RichConnection				sourceConnection;
	private RichConnection				targetConnection;
	private QCSampleConstructor			qcSampleConstructor;
	private EtlReport					etlReport;
	private CDMV4NullableChecker		cdmv4NullableChecker		= new CDMV4NullableChecker();
	private OneToManyList<String, Row>	tableToRows;
	private long						personCount;
	private long						observationPeriodId;
	private Integer						locationId;
	private long						drugExposureId;
	private long						conditionOccurrenceId;
	private long						visitOccurrenceId;
	private long						procedureOccurrenceId;
	private long						procedureCostId;
	private long						visitStartDate;
	private long						visitEndDate;
	private long						primaryProcedureOccurrenceId;
	
	private Map<String, Integer>		stateCountyToLocationId;
	private Set<Integer>				careSiteIds;
	
	private Map<String, String>			codeToCounty;
	private CodeToConceptMap			icd9ToConcept;
	private CodeToConceptMap			icd9ToRxNorm;
	private CodeToConceptMap			icd9ProcToConcept;
	private CodeToConceptMap			drgToConcept;
	
	public void process(String folder, DbSettings sourceDbSettings, DbSettings targetDbSettings, int maxPersons) {
		// Hardcode vocab server for now:
		DbSettings vocabDbSettings = new DbSettings();
		vocabDbSettings.database = "vocabulary";
		vocabDbSettings.dataType = DbSettings.DATABASE;
		vocabDbSettings.server = "127.0.0.1";
		vocabDbSettings.dbType = DbType.MYSQL;
		vocabDbSettings.user = "root";
		vocabDbSettings.password = "F1r3starter";
		
		if (ETLUtils.databaseAlreadyExists(targetDbSettings))
			return;
		CdmV4.createStructure(targetDbSettings);
		
		sourceConnection = new RichConnection(sourceDbSettings.server, sourceDbSettings.domain, sourceDbSettings.user, sourceDbSettings.password,
				sourceDbSettings.dbType);
		sourceConnection.setContext(this.getClass());
		sourceConnection.use(sourceDbSettings.database);
		
		targetConnection = new RichConnection(targetDbSettings.server, targetDbSettings.domain, targetDbSettings.user, targetDbSettings.password,
				targetDbSettings.dbType);
		targetConnection.setContext(this.getClass());
		targetConnection.use(targetDbSettings.database);
		
		loadMappings(vocabDbSettings);
		
		QCSampleConstructor.sampleProbability = 0.000001;
		qcSampleConstructor = new QCSampleConstructor(folder + "/sample");
		tableToRows = new OneToManyList<String, Row>();
		etlReport = new EtlReport(folder);
		stateCountyToLocationId = new HashMap<String, Integer>();
		careSiteIds = new HashSet<Integer>();
		personCount = 0;
		drugExposureId = 0;
		conditionOccurrenceId = 0;
		visitOccurrenceId = 0;
		procedureOccurrenceId = 0;
		procedureCostId = 0;
		observationPeriodId = 0;
		
		StringUtilities.outputWithTime("Processing persons");
		for (Row row : sourceConnection.query("SELECT * FROM core")) {
			processPerson(row);
			if (personCount == maxPersons) {
				System.out.println("Reached limit of " + maxPersons + " persons, terminating");
				break;
			}
			if (personCount % BATCH_SIZE == 0) {
				insertBatch();
				System.out.println("Processed " + personCount + " persons");
			}
		}
		insertBatch();
		System.out.println("Processed " + personCount + " persons");
		CdmV4.createIndices(targetDbSettings);
		qcSampleConstructor.addCdmData(targetDbSettings, vocabDbSettings);
		String etlReportName = etlReport.generate(drgToConcept, icd9ProcToConcept, icd9ToConcept, icd9ToRxNorm);
		System.out.println("An ETL report was generated and written to :" + etlReportName);
		if (etlReport.getTotalProblemCount() > 0) {
			String etlProblemListname = etlReport.generateProblemList();
			System.out.println("An ETL problem list was generated and written to :" + etlProblemListname);
		}
		StringUtilities.outputWithTime("Finished ETL");
	}
	
	private void processPerson(Row row) {
		etlReport.registerIncomingData("core", row);
		
		personCount++;
		visitOccurrenceId++;
		observationPeriodId++;
		primaryProcedureOccurrenceId = -1;
		visitStartDate = computeVisitStartDate(row.get("YEAR"), row.get("AMONTH"), row.get("AWEEKEND"));
		visitEndDate = computeVisitEndDate(visitStartDate, row.get("LOS"));
		
		qcSampleConstructor.registerPersonData("core", row, row.getLong("KEY"));
		
		addToLocation(row);
		addToCareSite(row);
		addToVisitOccurrence(row);
		addToObservationPeriod(row);
		addToPerson(row);
		addToConditionOccurrence(row);
		addToDeath(row);
		addToDrugExposure(row);
		addToProcedureOccurrence(row);
		addToProcedureCost(row);
	}
	
	private void addToLocation(Row row) {
		String stateCounty = row.get("HOSPST") + "\t" + (row.get("HOSPSTCO").equals("-9999") ? "" : row.get("HOSPSTCO"));
		locationId = stateCountyToLocationId.get(stateCounty);
		if (locationId == null) {
			locationId = stateCountyToLocationId.size() + 1;
			stateCountyToLocationId.put(stateCounty, locationId);
			
			Row location = new Row();
			location.add("location_id", locationId);
			location.add("state", row.get("HOSPST"));
			String county = codeToCounty.get(row.get("HOSPSTCO"));
			if (county == null)
				county = "";
			if (county.length() > 20)
				county = county.substring(0, 20); // County field in CDM limited to 20 chars
			location.add("county", county);
			location.add("location_source_value", row.get("HOSPSTCO"));
			tableToRows.put("location", location);
		}
	}
	
	private void addToCareSite(Row row) {
		if (careSiteIds.add(row.getInt("HOSPID"))) {
			Row careSite = new Row();
			careSite.add("care_site_id", row.get("HOSPID"));
			careSite.add("care_site_source_value", row.get("HOSPID"));
			careSite.add("location_id", locationId);
			tableToRows.put("care_site", careSite);
		}
	}
	
	private void addToVisitOccurrence(Row row) {
		Row visitOccurrence = new Row();
		visitOccurrence.add("person_id", row.get("KEY"));
		visitOccurrence.add("visit_occurrence_id", visitOccurrenceId);
		visitOccurrence.add("visit_start_date", StringUtilities.daysToDatabaseDateString(visitStartDate));
		visitOccurrence.add("visit_end_date", StringUtilities.daysToDatabaseDateString(visitEndDate));
		visitOccurrence.add("care_site_id", row.get("HOSPID"));
		visitOccurrence.add("place_of_service_concept_id", 9201); // Inpatient visit
		tableToRows.put("visit_occurrence", visitOccurrence);
	}
	
	private void addToObservationPeriod(Row row) {
		Row observationPeriod = new Row();
		observationPeriod.add("observation_period_id", observationPeriodId);
		observationPeriod.add("person_id", row.get("KEY"));
		observationPeriod.add("observation_period_start_date", StringUtilities.daysToDatabaseDateString(visitStartDate));
		observationPeriod.add("observation_period_end_date", StringUtilities.daysToDatabaseDateString(visitEndDate));
		tableToRows.put("observation_period", observationPeriod);
	}
	
	private void addToPerson(Row row) {
		if (row.getInt("AGE") < 0 || (row.getInt("AGE") == 0 && row.getInt("AGEDAY") < 0)) { // No age specified. Cannot create person, since birth year is
																								// required field
			etlReport.reportProblem("Person", "No age specified so cannot create row", row.get("KEY"));
			return;
		}
		
		Row person = new Row();
		person.add("person_id", row.get("KEY"));
		person.add("person_source_value", row.get("KEY"));
		person.add("gender_source_value", row.get("FEMALE"));
		person.add("gender_concept_id", row.get("FEMALE").equals("1") ? "8532" : row.get("FEMALE").equals("0") ? "8507" : "8551");
		
		if (row.getInt("AGE") > 0) {
			int yearOfBirth = Integer.parseInt(StringUtilities.daysToCalendarYear(visitStartDate)) - row.getInt("AGE");
			person.add("year_of_birth", yearOfBirth);
			person.add("month_of_birth", "");
			person.add("day_of_birth", "");
			
		} else if (row.getInt("AGEDAY") >= 0) {
			long dateOfBirth = visitStartDate - row.getInt("AGEDAY");
			person.add("year_of_birth", StringUtilities.daysToCalendarYear(dateOfBirth));
			person.add("month_of_birth", StringUtilities.daysToCalendarMonth(dateOfBirth));
			person.add("day_of_birth", StringUtilities.daysToCalendarDayOfMonth(dateOfBirth));
		} else {
			person.add("year_of_birth", "");
			person.add("month_of_birth", "");
			person.add("day_of_birth", "");
		}
		
		person.add("race_source_value", row.get("RACE"));
		if (row.get("RACE").equals("1")) // White
			person.add("race_concept_id", "8527");
		else if (row.get("RACE").equals("2")) // Black
			person.add("race_concept_id", "8516");
		else if (row.get("RACE").equals("4")) // Pacific islander
			person.add("race_concept_id", "8557");
		else if (row.get("RACE").equals("5")) // Native American
			person.add("race_concept_id", "8657");
		else if (row.get("RACE").equals("3")) // Hispanic, should be coded as 'other'
			person.add("race_concept_id", "8522");
		else if (row.get("RACE").equals("6")) // Other
			person.add("race_concept_id", "8522");
		else
			person.add("race_concept_id", "");
		
		if (row.get("RACE").equals("3")) {// Hispanic
			person.add("ethnicity_source_value", "3");
			person.add("ethnicity_concept_id", "38003563");
		} else {
			person.add("ethnicity_source_value", "");
			person.add("ethnicity_concept_id", "");
		}
		
		tableToRows.put("person", person);
	}
	
	private void addToConditionOccurrence(Row row) {
		for (int i = 0; i < diagnoseFields.length; i++)
			if (row.get(diagnoseFields[i]).trim().length() != 0) {
				Row conditionOccurrence = new Row();
				conditionOccurrence.add("person_id", row.get("KEY"));
				conditionOccurrence.add("condition_occurrence_id", ++conditionOccurrenceId);
				conditionOccurrence.add("condition_source_value", row.get(diagnoseFields[i]));
				conditionOccurrence.add("condition_concept_id", icd9ToConcept.getConceptId(row.get(diagnoseFields[i]).trim()));
				conditionOccurrence.add("condition_type_concept_id", diagnoseFieldConceptIds[i]);
				conditionOccurrence.add("condition_start_date", StringUtilities.daysToDatabaseDateString(visitStartDate));
				conditionOccurrence.add("visit_occurrence_id", visitOccurrenceId);
				tableToRows.put("condition_occurrence", conditionOccurrence);
			}
	}
	
	private void addToDeath(Row row) {
		if (row.get("DIED").equals("1")) {
			Row death = new Row();
			death.add("person_id", row.get("KEY"));
			death.add("death_date", StringUtilities.daysToDatabaseDateString(visitEndDate));
			death.add("death_type_concept_id", 38003566); // EHR record patient status "Deceased"
			tableToRows.put("death", death);
		}
	}
	
	private void addToDrugExposure(Row row) {
		for (int i = 0; i < procedureFields.length; i++)
			if (row.get(procedureFields[i]).trim().length() != 0) {
				int conceptId = icd9ToRxNorm.getConceptId(row.get(procedureFields[i]).trim());
				if (conceptId != 0) {
					Row drugExposure = new Row();
					drugExposure.add("drug_exposure_id", ++drugExposureId);
					drugExposure.add("person_id", row.get("KEY"));
					int day = row.getInt(procedureDayFields[i]);
					if (day < 0)
						day = 0;
					drugExposure.add("drug_exposure_start_date", StringUtilities.daysToDatabaseDateString(visitStartDate + day));
					drugExposure.add("drug_source_value", row.get(procedureFields[i]));
					drugExposure.add("drug_concept_id", conceptId);
					drugExposure.add("drug_type_concept_id", 38000179); // Physician administered drug (identified as procedure)
					drugExposure.add("visit_occurrence_id", visitOccurrenceId);
					tableToRows.put("drug_exposure", drugExposure);
				}
			}
	}
	
	private void addToProcedureOccurrence(Row row) {
		for (int i = 0; i < procedureFields.length; i++)
			if (row.get(procedureFields[i]).trim().length() != 0) {
				Row procedureOccurrence = new Row();
				procedureOccurrence.add("procedure_occurrence_id", ++procedureOccurrenceId);
				procedureOccurrence.add("person_id", row.get("KEY"));
				int day = row.getInt(procedureDayFields[i]);
				if (day < 0)
					day = 0;
				procedureOccurrence.add("procedure_date", StringUtilities.daysToDatabaseDateString(visitStartDate + day));
				procedureOccurrence.add("procedure_source_value", row.get(procedureFields[i]));
				procedureOccurrence.add("procedure_concept_id", icd9ProcToConcept.getConceptId(row.get(procedureFields[i]).trim()));
				procedureOccurrence.add("procedure_type_concept_id", procedureFieldConceptIds[i]);
				procedureOccurrence.add("visit_occurrence_id", visitOccurrenceId);
				tableToRows.put("procedure_occurrence", procedureOccurrence);
				
				if (procedureFields[i].equals("PR1"))
					primaryProcedureOccurrenceId = procedureOccurrenceId;
			}
	}
	
	private void addToProcedureCost(Row row) {
		if (primaryProcedureOccurrenceId != -1) {
			Row procedureCost = new Row();
			procedureCost.add("procedure_occurrence_id", primaryProcedureOccurrenceId);
			procedureCost.add("procedure_cost_id", ++procedureCostId);
			procedureCost.add("disease_class_source_value", row.get("DRG"));
			procedureCost.add("disease_class_concept_id", drgToConcept.getConceptId(row.get("DRG").trim()));
			tableToRows.put("procedure_cost", procedureCost);
		}
	}
	
	private long computeVisitStartDate(String year, String amonth, String aweekend) {
		if (Integer.parseInt(amonth) < 1)
			amonth = "1";
		boolean isWeekend = aweekend.equals("1");
		Calendar calendar = Calendar.getInstance();
		calendar.set(Integer.parseInt(year), Integer.parseInt(amonth) - 1, 1);
		while (isWeekend(calendar) != isWeekend)
			calendar.add(Calendar.DATE, 1);
		long time = calendar.getTimeInMillis();
		time += calendar.getTimeZone().getOffset(time);
		return (((StringUtilities.MILLENIUM + time) / StringUtilities.DAY) - (1000 * 365));
	}
	
	private long computeVisitEndDate(long visitStartDate, String los) {
		int lengthOfStay = Integer.parseInt(los);
		if (lengthOfStay < 0)
			lengthOfStay = 0;
		return visitStartDate + lengthOfStay;
	}
	
	private boolean isWeekend(Calendar calendar) {
		int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
		return (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY);
	}
	
	private void insertBatch() {
		// addToDrugEra();
		// addToConditionEra();
		removeRowsWithNonNullableNulls();
		
		etlReport.registerOutgoingData(tableToRows);
		for (String table : tableToRows.keySet())
			targetConnection.insertIntoTable(tableToRows.get(table).iterator(), table, false);
		tableToRows.clear();
	}
	
	private void loadMappings(DbSettings dbSettings) {
		StringUtilities.outputWithTime("Loading mappings from server");
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		
		connection.use("vocabulary");
		
		System.out.println("- Loading ICD-9 to SNOMED concept_id mapping");
		icd9ToConcept = new CodeToConceptMap("ICD-9 to SNOMED concept_id mapping");
		String query = "SELECT source_code,source_code_description,target_concept_id,concept_code,concept_name FROM source_to_concept_map INNER JOIN concept ON target_concept_id = concept_id WHERE source_vocabulary_id = 2 AND target_vocabulary_id = 1 AND concept_class='Clinical finding'";
		for (Row row : connection.query(query))
			icd9ToConcept.add(row.get("SOURCE_CODE").replace(".", ""), row.get("SOURCE_CODE_DESCRIPTION"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("CONCEPT_CODE"), row.get("CONCEPT_NAME"));
		
		System.out.println("- Loading ICD-9 to RxNorm concept_id mapping");
		icd9ToRxNorm = new CodeToConceptMap("ICD-9 to RxNorm concept_id mapping");
		query = "SELECT DISTINCT source_code,source_code_description,target_concept_id,concept_code,concept_name FROM source_to_concept_map INNER JOIN concept ON target_concept_id = concept_id WHERE  target_vocabulary_id = 8 AND source_vocabulary_id in (3,4,5) AND primary_map = 'Y' AND source_to_concept_map.invalid_reason != 'D'";
		for (Row row : connection.query(query))
			icd9ToRxNorm.add(row.get("SOURCE_CODE").replace(".", ""), row.get("SOURCE_CODE_DESCRIPTION"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("CONCEPT_CODE"), row.get("CONCEPT_NAME"));
		
		System.out.println("- Loading ICD-9 Procedure to concept_id mapping");
		icd9ProcToConcept = new CodeToConceptMap("ICD-9 Procedure to concept_id mapping");
		query = "SELECT concept_id,concept_name,concept_code FROM concept WHERE vocabulary_id = 3";
		for (Row row : connection.query(query))
			icd9ProcToConcept.add(row.get("CONCEPT_CODE").replace(".", ""), row.get("CONCEPT_NAME"), row.getInt("CONCEPT_ID"), row.get("CONCEPT_CODE"),
					row.get("CONCEPT_NAME"));
		
		System.out.println("- Loading DRG to concept_id mapping");
		drgToConcept = new CodeToConceptMap("DRG to concept_id mapping");
		query = "SELECT concept_id,concept_name,concept_code FROM concept WHERE vocabulary_id = 40 AND concept_class = 'DRG'";
		for (Row row : connection.query(query))
			drgToConcept.add(row.get("CONCEPT_CODE"), row.get("CONCEPT_NAME"), row.getInt("CONCEPT_ID"), row.get("CONCEPT_CODE"), row.get("CONCEPT_NAME"));
		
		System.out.println("- Loading county code to name mapping");
		codeToCounty = new HashMap<String, String>();
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("national_county.txt")))
			codeToCounty.put(row.get("State ANSI") + row.get("County ANSI"), row.get("County Name"));
		
		StringUtilities.outputWithTime("Finished loading mappings");
	}
	
	private void removeRowsWithNonNullableNulls() {
		for (String table : tableToRows.keySet()) {
			Iterator<Row> iterator = tableToRows.get(table).iterator();
			while (iterator.hasNext()) {
				Row row = iterator.next();
				String nonAllowedNullField = cdmv4NullableChecker.findNonAllowedNull(table, row);
				if (nonAllowedNullField != null) {
					if (row.getFieldNames().contains("person_id"))
						etlReport.reportProblem(table, "Column " + nonAllowedNullField + " is null, could not create row", row.get("person_id"));
					else
						etlReport.reportProblem(table, "Column " + nonAllowedNullField + " is null, could not create row", "");
					iterator.remove();
				}
			}
		}
	}
}
