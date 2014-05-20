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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ohdsi.databases.RichConnection;
import org.ohdsi.ooxml.ReadXlsxFileWithHeader;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.OneToManyList;
import org.ohdsi.utilities.files.FileSorter;
import org.ohdsi.utilities.files.MultiRowIterator;
import org.ohdsi.utilities.files.MultiRowIterator.MultiRowSet;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.RowUtilities;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.EtlReport;
import org.ohdsi.whiteRabbit.ObjectExchange;
import org.ohdsi.whiteRabbit.cdm.CDMV4NullableChecker;
import org.ohdsi.whiteRabbit.cdm.CdmV4;
import org.ohdsi.whiteRabbit.utilities.CSVFileChecker;
import org.ohdsi.whiteRabbit.utilities.CodeToConceptMap;
import org.ohdsi.whiteRabbit.utilities.ConditionEraGenerator;
import org.ohdsi.whiteRabbit.utilities.DrugEraGenerator;
import org.ohdsi.whiteRabbit.utilities.ETLUtils;
import org.ohdsi.whiteRabbit.utilities.QCSampleConstructor;

public class ARSETL {
	
	public static String				EXTRACTION_DATE					= "2012-12-31";
	public static String				MIN_OBSERVATION_DATE			= "2003-01-01";
	public static int					MAX_ROWS_PER_PERSON				= 100000;
	public static String[]				TABLES							= new String[] { "DDRUG", "DRUGS", "EXE", "HOSP", "OUTPAT", "PERSON" };
	public static int					BATCH_SIZE						= 100;
	
	private int							personCount;
	private String						folder;
	private RichConnection				connection;
	private long						personId;
	private long						observationPeriodId;
	private long						drugExposureId;
	private long						conditionOccurrenceId;
	private long						visitOccurrenceId;
	private long						procedureOccurrenceId;
	private long						procedureCostId;
	private long						extractionDate;
	private long						minObservationDate;
	private OneToManyList<String, Row>	tableToRows;
	private CodeToConceptMap			atcToConcept;
	private CodeToConceptMap			atcToRxNormConcepts;
	private Map<String, String>			procToType;
	private CodeToConceptMap			icd9ToConcept;
	private CodeToConceptMap			icd9ProcToConcept;
	private CodeToConceptMap			specialtyToConcept;
	private Map<String, Long>			hospRowToVisitOccurrenceId		= new HashMap<String, Long>();
	private Map<String, Long>			outpatRowToVisitOccurrenceId	= new HashMap<String, Long>();
	private Map<String, Long>			hospRowToProcedureOccurrenceId	= new HashMap<String, Long>();
	private Set<Integer>				providerIds;
	private QCSampleConstructor			qcSampleConstructor;
	private EtlReport					etlReport;
	private CDMV4NullableChecker		cdmv4NullableChecker			= new CDMV4NullableChecker();
	
	public void process(String folder, DbSettings dbSettings, int maxPersons) {
		this.folder = folder;
		extractionDate = StringUtilities.databaseTimeStringToDays(EXTRACTION_DATE);
		minObservationDate = StringUtilities.databaseTimeStringToDays(MIN_OBSERVATION_DATE);
		System.out.println("Extraction date is set at " + EXTRACTION_DATE);
		System.out.println("Minimum observation date is set at " + MIN_OBSERVATION_DATE);
		
		if (ETLUtils.databaseAlreadyExists(dbSettings))
			return;
		// checkTablesForFormattingErrors();
		// sortTables();
		CdmV4.createStructure(dbSettings);
		connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(this.getClass());
		connection.use(dbSettings.database);
		loadMappings(dbSettings);
		
		personCount = 0;
		personId = 0;
		observationPeriodId = 0;
		drugExposureId = 0;
		conditionOccurrenceId = 0;
		visitOccurrenceId = 0;
		procedureOccurrenceId = 0;
		procedureCostId = 0;
		providerIds = new HashSet<Integer>();
		qcSampleConstructor = new QCSampleConstructor(folder + "/sample");
		tableToRows = new OneToManyList<String, Row>();
		etlReport = new EtlReport(folder);
		
		StringUtilities.outputWithTime("Processing persons");
		MultiRowIterator iterator = constructMultiRowIterator();
		while (iterator.hasNext()) {
			processPerson(iterator.next());
			
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
		CdmV4.createIndices(dbSettings);
		qcSampleConstructor.addCdmData(dbSettings, dbSettings);
		String etlReportName = etlReport.generate(atcToConcept, icd9ProcToConcept, icd9ToConcept, atcToRxNormConcepts, specialtyToConcept);
		System.out.println("An ETL report was generated and written to :" + etlReportName);
		if (etlReport.getTotalProblemCount() > 0) {
			String etlProblemListname = etlReport.generateProblemList();
			System.out.println("An ETL problem list was generated and written to :" + etlProblemListname);
		}
		StringUtilities.outputWithTime("Finished ETL");
	}
	
	private void checkTablesForFormattingErrors() {
		StringUtilities.outputWithTime("Checking tables for formatting errors");
		
		// for (String table : TABLES) {
		// StringUtilities.outputWithTime("- Checking " + table);
		CSVFileChecker checker = new CSVFileChecker();
		checker.setFrame(ObjectExchange.frame);
		// checker.checkFile(folder + "/" + table + ".csv");
		checker.checkSpecifiedFields(folder, new ReadXlsxFileWithHeader(this.getClass().getResourceAsStream("Fields.xlsx")).iterator());
		// }
		StringUtilities.outputWithTime("Finished checking tables");
	}
	
	private void sortTables() {
		StringUtilities.outputWithTime("Sorting tables");
		for (String table : TABLES) {
			StringUtilities.outputWithTime("- Sorting " + table);
			FileSorter.sort(folder + "/" + table + ".csv", "PERSON_ID");
		}
		StringUtilities.outputWithTime("Finished sorting");
	}
	
	private void loadMappings(DbSettings dbSettings) {
		StringUtilities.outputWithTime("Loading mappings from server");
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		
		connection.use("vocabulary");
		
		System.out.println("- Loading ATC to concept_id mapping");
		atcToConcept = new CodeToConceptMap("ATC to concept_id mapping");
		String query = "SELECT concept_id,concept_name,concept_code FROM concept WHERE VOCABULARY_ID = 21";
		for (Row row : connection.query(query))
			atcToConcept.add(row.get("CONCEPT_CODE"), row.get("CONCEPT_NAME"), row.getInt("CONCEPT_ID"), row.get("CONCEPT_CODE"), row.get("CONCEPT_NAME"));
		
		System.out.println("- Loading ICD-9 to concept_id mapping");
		icd9ToConcept = new CodeToConceptMap("ICD-9 to concept_id mapping");
		query = "SELECT source_code,source_code_description,target_concept_id,concept_code,concept_name FROM source_to_concept_map INNER JOIN concept ON target_concept_id = concept_id WHERE source_vocabulary_id = 2 AND target_vocabulary_id = 1";
		for (Row row : connection.query(query)) {
			icd9ToConcept.add(row.get("SOURCE_CODE"), row.get("SOURCE_CODE_DESCRIPTION"), row.getInt("TARGET_CONCEPT_ID"), row.get("CONCEPT_CODE"),
					row.get("CONCEPT_NAME"));
			icd9ToConcept.add(row.get("SOURCE_CODE").replace(".", ""), row.get("SOURCE_CODE_DESCRIPTION"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("CONCEPT_CODE"), row.get("CONCEPT_NAME"));
		}
		
		System.out.println("- Loading ICD-9 Procedure to concept_id mapping");
		icd9ProcToConcept = new CodeToConceptMap("ICD-9 Procedure to concept_id mapping");
		query = "SELECT concept_id,concept_name,concept_code FROM concept WHERE VOCABULARY_ID = 3";
		for (Row row : connection.query(query)) {
			icd9ProcToConcept.add(row.get("CONCEPT_CODE"), row.get("CONCEPT_NAME"), row.getInt("CONCEPT_ID"), row.get("CONCEPT_CODE"), row.get("CONCEPT_NAME"));
			icd9ProcToConcept.add(row.get("CONCEPT_CODE").replace(".", ""), row.get("CONCEPT_NAME"), row.getInt("CONCEPT_ID"), row.get("CONCEPT_CODE"),
					row.get("CONCEPT_NAME"));
		}
		
		System.out.println("- Loading ATC to RxNorm concept_id mapping");
		atcToRxNormConcepts = new CodeToConceptMap("ATC to RxNorm ingredient mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("AtcToRxNormIngredient.csv")))
			atcToRxNormConcepts.add(row.get("atc"), row.get("atc_name"), row.getInt("rxnorm_concept_id"), row.get("rxnorm_code"),
					row.get("rxnorm_concept name"));
		
		System.out.println("- Loading specialty to concept mapping");
		specialtyToConcept = new CodeToConceptMap("Specialty to concept mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("specialty_italian.csv")))
			specialtyToConcept.add(removeLeadingZeroes(row.get("COD")), row.get("DESCRIZIONE"), row.getInt("CONCEPT_ID"), row.get("CONCEPT_CODE"),
					row.get("CONCEPT_NAME"));
		
		System.out.println("- Loading proc_cod to type_outpat mapping");
		procToType = new HashMap<String, String>();
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("proc_OUTPAT.csv")))
			procToType.put(row.get("PROC_COD"), row.get("TYPE_OUTPAT"));
		
		StringUtilities.outputWithTime("Finished loading mappings");
	}
	
	private MultiRowIterator constructMultiRowIterator() {
		@SuppressWarnings("unchecked")
		Iterator<Row>[] iterators = new Iterator[TABLES.length];
		for (int i = 0; i < TABLES.length; i++)
			iterators[i] = new ReadCSVFileWithHeader(folder + "/" + TABLES[i] + ".csv").iterator();
		return new MultiRowIterator("PERSON_ID", TABLES, iterators);
	}
	
	private void processPerson(MultiRowSet personData) {
		etlReport.registerIncomingData(personData);
		
		if (personData.linkingId.trim().length() == 0) {
			for (String table : personData.keySet())
				if (personData.get(table).size() > 0)
					etlReport.reportProblem(table, "Missing person_id in " + personData.get(table).size() + " rows in table " + table, "");
			return;
		}
		
		if (personData.totalSize() > MAX_ROWS_PER_PERSON) {
			etlReport.reportProblem("", "Person has too many rows (" + personData.totalSize() + "). Skipping person", personData.linkingId);
			return;
		}
		
		personCount++;
		personId++;
		
		qcSampleConstructor.registerPersonData(personData, personId);
		fixDateProblem(personData);
		
		hospRowToVisitOccurrenceId.clear();
		outpatRowToVisitOccurrenceId.clear();
		hospRowToProcedureOccurrenceId.clear();
		addToProvider(personData);
		addToPerson(personData);
		addToObservationPeriod(personData);
		addToDeath(personData);
		addToVisitOccurrence(personData);
		addToDrugExposure(personData);
		addToConditionOccurrence(personData);
		addToProcedureOccurrence(personData);
		addToProcedureCost(personData);
	}
	
	private void fixDateProblem(MultiRowSet personData) {
		for (List<Row> rows : personData.values())
			if (rows.size() != 0) {
				List<String> dateFields = new ArrayList<String>();
				for (String fieldName : rows.get(0).getFieldNames())
					if (fieldName.toLowerCase().startsWith("date") || fieldName.toLowerCase().endsWith("date"))
						dateFields.add(fieldName);
				for (Row row : rows)
					for (String fieldName : dateFields) {
						String value = row.get(fieldName);
						if (CSVFileChecker.isDateFormat1(value))
							row.set(fieldName, value.substring(0, 10));
					}
			}
	}
	
	private void insertBatch() {
		addToDrugEra();
		addToConditionEra();
		removeRowsWithNonNullableNulls();
		
		etlReport.registerOutgoingData(tableToRows);
		for (String table : tableToRows.keySet())
			connection.insertIntoTable(tableToRows.get(table).iterator(), table, false);
		tableToRows.clear();
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
	
	private void addToProvider(MultiRowSet personData) {
		for (Row row : personData.get("OUTPAT")) {
			if (row.get("GROUP_CODE").length() > 0) {
				int providerId = row.getInt("GROUP_CODE"); // Provider ID for outpatient providers is <group_code>
				if (providerIds.add(providerId)) { // Did not encounter this group code before
					int specialtyConceptId = specialtyToConcept.getConceptId(removeLeadingZeroes(row.get("GROUP_CODE")));
					
					Row provider = new Row();
					provider.add("provider_id", providerId);
					provider.add("specialty_concept_id", specialtyConceptId);
					provider.add("specialty_source_value", row.get("GROUP_CODE"));
					provider.add("provider_source_value", "");
					tableToRows.put("provider", provider);
				}
			}
		}
		
		for (Row row : personData.get("HOSP")) {
			if (row.get("WARD_DISCHARGE").length() > 0) {
				int providerId = row.getInt("WARD_DISCHARGE") + 1000; // Provider ID for hosp providers is <ward_discharge> + 1000;
				if (providerIds.add(providerId)) { // Did not encounter this specialty code before
					String specialtyCode = row.get("WARD_DISCHARGE").substring(0, 2);
					int specialtyConceptId = specialtyToConcept.getConceptId(removeLeadingZeroes(specialtyCode));
					
					Row provider = new Row();
					provider.add("provider_id", providerId);
					provider.add("specialty_concept_id", specialtyConceptId);
					provider.add("specialty_source_value", specialtyCode);
					provider.add("provider_source_value", row.get("WARD_DISCHARGE"));
					tableToRows.put("provider", provider);
				}
			}
		}
		
		for (Row row : personData.get("PERSON")) {
			if (row.get("GP_ID").length() != 0) {
				int providerId = row.getInt("GP_ID") + 100000; // Provider ID for gp providers is <gp_id> + 100000;
				if (providerIds.add(providerId)) { // Did not encounter this gp_id before
					Row provider = new Row();
					provider.add("provider_id", providerId);
					provider.add("specialty_concept_id", 38004446); // General Practice
					provider.add("specialty_source_value", "");
					provider.add("provider_source_value", row.get("GP_ID"));
					tableToRows.put("provider", provider);
				}
			}
		}
	}
	
	private void addToProcedureOccurrence(MultiRowSet personData) {
		for (Row row : personData.get("HOSP")) {
			String startDate = row.get("START_DATE");
			String mainProcDate = row.get("DATE_MAIN_PROC");
			String providerId = "";
			if (row.get("WARD_DISCHARGE").length() != 0)
				providerId = Integer.toString(row.getInt("WARD_DISCHARGE") + 1000); // Provider ID for hosp providers is <ward_discharge> + 1000;
			if (mainProcDate.length() == 0)
				mainProcDate = startDate;
			Long visitId = hospRowToVisitOccurrenceId.get(row.toString());
			hospRowToProcedureOccurrenceId.put(row.toString(), procedureOccurrenceId);
			addHospProcedure(row.get("MAIN_PROC"), mainProcDate, visitId, providerId, 38000248);
			addHospProcedure(row.get("SECONDARY_PROC_1"), startDate, visitId, providerId, 38000249);
			addHospProcedure(row.get("SECONDARY_PROC_2"), startDate, visitId, providerId, 38000249);
			addHospProcedure(row.get("SECONDARY_PROC_3"), startDate, visitId, providerId, 38000249);
			addHospProcedure(row.get("SECONDARY_PROC_4"), startDate, visitId, providerId, 38000249);
			addHospProcedure(row.get("SECONDARY_PROC_5"), startDate, visitId, providerId, 38000249);
		}
		
		for (Row row : personData.get("OUTPAT")) {
			String procDate = row.get("PROC_START_DATE");
			if (procDate.length() == 0)
				procDate = row.get("PROC_DATE");
			if (procDate.length() != 0) { // Currently suppressing problems due to missing dates
				Row procedureOccurrence = new Row();
				procedureOccurrence.add("procedure_occurrence_id", procedureOccurrenceId++);
				procedureOccurrence.add("person_id", personId);
				procedureOccurrence.add("procedure_concept_id", 0);
				procedureOccurrence.add("procedure_date", procDate);
				procedureOccurrence.add("procedure_type_concept_id", 38000266);
				Long visitId = outpatRowToVisitOccurrenceId.get(row.toString());
				procedureOccurrence.add("visit_occurrence_id", (visitId == null ? "" : visitId.toString()));
				procedureOccurrence.add("procedure_source_value", row.get("PROC_COD"));
				procedureOccurrence.add("associated_provider_id", row.get("GROUP_CODE")); // Provider ID for outpatient providers is <group_code>
				tableToRows.put("procedure_occurrence", procedureOccurrence);
			}
		}
	}
	
	private void addToProcedureCost(MultiRowSet personData) {
		for (Row row : personData.get("HOSP"))
			if (row.get("DRG").length() != 0) {
				Row procedureCost = new Row();
				procedureCost.add("procedure_cost_id", procedureCostId++);
				Long procedureOccurrenceId = hospRowToProcedureOccurrenceId.get(row.toString());
				procedureCost.add("procedure_occurrence_id", (procedureOccurrenceId == null ? "" : procedureOccurrenceId.toString()));
				procedureCost.add("disease_class_source_value", row.get("DRG"));
				tableToRows.put("procedure_cost", procedureCost);
			}
	}
	
	private void addHospProcedure(String proc, String date, Long visitId, String providerId, int procedureTypeConceptId) {
		proc = proc.trim();
		if (proc.length() != 0) {
			Row procedureOccurrence = new Row();
			procedureOccurrence.add("procedure_occurrence_id", procedureOccurrenceId++);
			procedureOccurrence.add("person_id", personId);
			procedureOccurrence.add("procedure_concept_id", icd9ProcToConcept.getConceptId(proc));
			procedureOccurrence.add("procedure_date", date);
			procedureOccurrence.add("procedure_type_concept_id", procedureTypeConceptId);
			procedureOccurrence.add("visit_occurrence_id", visitId);
			procedureOccurrence.add("procedure_source_value", proc);
			procedureOccurrence.add("associated_provider_id", providerId);
			tableToRows.put("procedure_occurrence", procedureOccurrence);
		}
	}
	
	private void addToVisitOccurrence(MultiRowSet personData) {
		for (Row row : personData.get("HOSP")) {
			hospRowToVisitOccurrenceId.put(row.toString(), visitOccurrenceId);
			
			Row visitOccurrence = new Row();
			visitOccurrence.add("visit_occurrence_id", visitOccurrenceId++);
			visitOccurrence.add("person_id", personId);
			visitOccurrence.add("visit_start_date", row.get("START_DATE"));
			if (row.get("END_DATE").length() == 0)
				visitOccurrence.add("visit_end_date", row.get("START_DATE"));
			else
				visitOccurrence.add("visit_end_date", row.get("END_DATE"));
			visitOccurrence.add("place_of_service_concept_id", 9201); // Inpatient visit
			tableToRows.put("visit_occurrence", visitOccurrence);
		}
		
		for (Row row : personData.get("OUTPAT")) {
			String type = procToType.get(row.get("PROC_COD"));
			if (type != null && type.equals("CLIN")) {
				outpatRowToVisitOccurrenceId.put(row.toString(), visitOccurrenceId);
				
				Row visitOccurrence = new Row();
				visitOccurrence.add("visit_occurrence_id", visitOccurrenceId++);
				visitOccurrence.add("person_id", personId);
				visitOccurrence.add("visit_start_date", row.get("PROC_START_DATE"));
				if (row.get("PROC_END_DATE").length() == 0)
					visitOccurrence.add("visit_end_date", row.get("PROC_START_DATE"));
				else
					visitOccurrence.add("visit_end_date", row.get("PROC_END_DATE"));
				visitOccurrence.add("place_of_service_concept_id", 9202); // Outpatient visit
				tableToRows.put("visit_occurrence", visitOccurrence);
			}
		}
	}
	
	private void addToConditionOccurrence(MultiRowSet personData) {
		for (Row row : personData.get("HOSP")) {
			String startDate = row.get("START_DATE");
			Long visitId = hospRowToVisitOccurrenceId.get(row.toString());
			String providerId = "";
			if (row.get("WARD_DISCHARGE").length() != 0)
				providerId = Integer.toString(row.getInt("WARD_DISCHARGE") + 1000); // Provider ID for hosp providers is <ward_discharge> + 1000;
			addHospCondition(startDate, row.get("MAIN_DIAGNOSIS"), visitId, providerId, 38000183); // Inpatient detail, primary
			addHospCondition(startDate, row.get("SECONDARY_DIAGNOSIS_1"), visitId, providerId, 38000184); // Inpatient detail - 1st position
			addHospCondition(startDate, row.get("SECONDARY_DIAGNOSIS_2"), visitId, providerId, 38000185); // Inpatient detail - 2st position
			addHospCondition(startDate, row.get("SECONDARY_DIAGNOSIS_3"), visitId, providerId, 38000186); // Inpatient detail - 3st position
			addHospCondition(startDate, row.get("SECONDARY_DIAGNOSIS_4"), visitId, providerId, 38000187); // Inpatient detail - 4st position
			addHospCondition(startDate, row.get("SECONDARY_DIAGNOSIS_5"), visitId, providerId, 38000188); // Inpatient detail - 5st position
		}
		
		for (Row row : personData.get("EXE")) {
			Row conditionOccurrence = new Row();
			conditionOccurrence.add("condition_occurrence_id", conditionOccurrenceId++);
			conditionOccurrence.add("person_id", personId);
			conditionOccurrence.add("condition_concept_id", icd9ToConcept.getConceptId(row.get("EXEMPTION_CODE")));
			conditionOccurrence.add("condition_start_date", row.get("EXE_START_DATE"));
			conditionOccurrence.add("condition_type_concept_id", 38000245);
			conditionOccurrence.add("visit_occurrence_id", "");
			conditionOccurrence.add("condition_source_value", row.get("EXEMPTION_CODE"));
			conditionOccurrence.add("associated_provider_id", "");
			tableToRows.put("condition_occurrence", conditionOccurrence);
		}
	}
	
	private void addToConditionEra() {
		ConditionEraGenerator conditionEraGenerator = new ConditionEraGenerator();
		for (Row row : tableToRows.get("condition_occurrence")) {
			int personId = row.getInt("person_id");
			long startDate = StringUtilities.databaseTimeStringToDays(row.get("condition_start_date"));
			long endDate = StringUtilities.MISSING_DATE;
			int conceptId = row.getInt("condition_concept_id");
			conditionEraGenerator.addCondition(personId, startDate, endDate, conceptId);
		}
		
		tableToRows.putAll("condition_era", conditionEraGenerator.generateRows());
	}
	
	private void addHospCondition(String startDate, String diagnose, Long visitId, String providerId, int typeConceptId) {
		diagnose = diagnose.trim();
		if (diagnose.length() != 0) {
			Row conditionOccurrence = new Row();
			conditionOccurrence.add("condition_occurrence_id", conditionOccurrenceId++);
			conditionOccurrence.add("person_id", personId);
			conditionOccurrence.add("condition_concept_id", icd9ToConcept.getConceptId(diagnose));
			conditionOccurrence.add("condition_start_date", startDate);
			conditionOccurrence.add("condition_type_concept_id", typeConceptId);
			conditionOccurrence.add("visit_occurrence_id", visitId);
			conditionOccurrence.add("condition_source_value", diagnose);
			conditionOccurrence.add("associated_provider_id", providerId);
			tableToRows.put("condition_occurrence", conditionOccurrence);
		}
	}
	
	private void addToDrugExposure(MultiRowSet personData) {
		for (Row row : personData.get("DRUGS")) {
			Row drugExposure = new Row();
			drugExposure.add("drug_exposure_id", drugExposureId++);
			drugExposure.add("person_id", personId);
			drugExposure.add("drug_concept_id", atcToConcept.getConceptId(row.get("ATC")));
			drugExposure.add("drug_exposure_start_date", row.get("DRUG_DISPENSING_DATE"));
			drugExposure.add("days_supply", row.get("DURATION"));
			drugExposure.add("drug_type_concept_id", 38000175);
			drugExposure.add("drug_source_value", row.get("PRODUCT_CODE"));
			drugExposure.add("atc", row.get("ATC")); // Store temporarily for drug era
			tableToRows.put("drug_exposure", drugExposure);
		}
		
		for (Row row : personData.get("DDRUG")) {
			Row drugExposure = new Row();
			drugExposure.add("drug_exposure_id", drugExposureId++);
			drugExposure.add("person_id", personId);
			drugExposure.add("drug_concept_id", atcToConcept.getConceptId(row.get("ATC")));
			drugExposure.add("drug_exposure_start_date", row.get("DRUG_DISPENSING_DATE"));
			drugExposure.add("days_supply", row.get("DURATION"));
			drugExposure.add("drug_type_concept_id", 38000175);
			drugExposure.add("drug_source_value", row.get("PRODUCT_CODE"));
			drugExposure.add("atc", row.get("ATC")); // Store temporarily for drug era
			tableToRows.put("drug_exposure", drugExposure);
		}
	}
	
	private void addToDrugEra() {
		DrugEraGenerator drugEraGenerator = new DrugEraGenerator();
		for (Row row : tableToRows.get("drug_exposure")) {
			int personId = row.getInt("person_id");
			long startDate = StringUtilities.databaseTimeStringToDays(row.get("drug_exposure_start_date"));
			long endDate = startDate + Math.round(parseZeroIfEmpty(row.get("days_supply")));
			for (int conceptId : atcToRxNormConcepts.getConceptIds(row.get("atc")))
				drugEraGenerator.addExposure(personId, startDate, endDate, conceptId);
			row.remove("atc");
		}
		
		tableToRows.putAll("drug_era", drugEraGenerator.generateRows());
	}
	
	private void addToDeath(MultiRowSet personData) {
		for (Row row : personData.get("PERSON")) {
			if (row.get("DATE_OF_DEATH").trim().length() != 0) {
				Row death = new Row();
				death.add("person_id", personId);
				death.add("death_type_concept_id", 38003565);
				death.add("death_date", row.get("DATE_OF_DEATH"));
				tableToRows.put("death", death);
			}
		}
	}
	
	private void addToObservationPeriod(MultiRowSet personData) {
		List<Row> rows = personData.get("PERSON");
		if (rows.size() == 0)
			return;
		
		RowUtilities.sort(rows, "STARTDATE");
		long observationPeriodStartDate = Long.MIN_VALUE;
		long observationPeriodEndDate = Long.MIN_VALUE;
		for (Row row : rows) {
			if (row.get("STARTDATE").length() == 0) {
				etlReport.reportProblem("PERSON", "No person startdate, could not create observation period", row.get("PERSON_ID"));
				continue;
			}
			long startDate = StringUtilities.databaseTimeStringToDays(row.get("STARTDATE"));
			startDate = Math.max(startDate, minObservationDate);
			long endDate = StringUtilities.databaseTimeStringToDays(row.get("ENDDATE"));
			endDate = endDate == StringUtilities.MISSING_DATE ? extractionDate : Math.min(endDate, extractionDate);
			if (observationPeriodEndDate == startDate - 1) {
				observationPeriodEndDate = endDate;
			} else {
				if (observationPeriodStartDate != Long.MIN_VALUE && observationPeriodStartDate < observationPeriodEndDate) {
					Row observationPeriod = new Row();
					observationPeriod.add("observation_period_id", observationPeriodId++);
					observationPeriod.add("person_id", personId);
					observationPeriod.add("observation_period_start_date", StringUtilities.daysToDatabaseDateString(observationPeriodStartDate));
					observationPeriod.add("observation_period_end_date", StringUtilities.daysToDatabaseDateString(observationPeriodEndDate));
					tableToRows.put("observation_period", observationPeriod);
				}
				observationPeriodStartDate = startDate;
				observationPeriodEndDate = endDate;
				
			}
		}
		if (observationPeriodStartDate != Long.MIN_VALUE && observationPeriodStartDate < observationPeriodEndDate) {
			Row observationPeriod = new Row();
			observationPeriod.add("observation_period_id", observationPeriodId++);
			observationPeriod.add("person_id", personId);
			observationPeriod.add("observation_period_start_date", StringUtilities.daysToDatabaseDateString(observationPeriodStartDate));
			observationPeriod.add("observation_period_end_date", StringUtilities.daysToDatabaseDateString(observationPeriodEndDate));
			tableToRows.put("observation_period", observationPeriod);
		}
	}
	
	private void addToPerson(MultiRowSet personData) {
		List<Row> rows = personData.get("PERSON");
		if (rows.size() == 0)
			return;
		
		Row row = rows.get(0);
		if (rows.size() > 1) { // Multiple rows: find the one with the latest startdate:
			RowUtilities.sort(rows, "STARTDATE");
			row = rows.get(rows.size() - 1);
		}
		
		Row person = new Row();
		person.add("person_id", personId);
		person.add("person_source_value", row.get("PERSON_ID"));
		String gender = row.get("GENDER_CONCEPT_ID");
		person.add("gender_source_value", gender);
		person.add("gender_concept_id", gender.toLowerCase().equals("m") || gender.toLowerCase().equals("1") ? "8507" : gender.toLowerCase().equals("f")
				|| gender.toLowerCase().equals("2") ? "8532" : "8521");
		String dateOfBirth = row.get("DATE_OF_BIRTH");
		
		if (dateOfBirth.length() < 10) {
			person.add("year_of_birth", "");
			person.add("month_of_birth", "");
			person.add("day_of_birth", "");
		} else {
			person.add("year_of_birth", dateOfBirth.substring(0, 4));
			person.add("month_of_birth", dateOfBirth.substring(5, 7));
			person.add("day_of_birth", dateOfBirth.substring(8, 10));
		}
		if (row.get("GP_ID").length() != 0)
			person.add("provider_id", row.getInt("GP_ID") + 100000);// Provider ID for gp providers is <gp_id> + 100000;
		else
			person.add("provider_id", "");
		person.add("location_id", row.get("LOCATION_CONCEPT_ID"));
		tableToRows.put("person", person);
	}
	
	private double parseZeroIfEmpty(String number) {
		if (number.length() == 0)
			return 0;
		else
			return Double.parseDouble(number);
	}
	
	private String removeLeadingZeroes(String string) {
		for (int i = 0; i < string.length(); i++)
			if (string.charAt(i) != '0')
				return string.substring(i);
		return string;
	}
}
