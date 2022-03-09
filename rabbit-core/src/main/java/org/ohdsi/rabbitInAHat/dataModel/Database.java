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
package org.ohdsi.rabbitInAHat.dataModel;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.ohdsi.utilities.ScanFieldName;
import org.ohdsi.utilities.ScanSheetName;
import org.ohdsi.utilities.files.QuickAndDirtyXlsxReader;
import org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Sheet;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.*;

public class Database implements Serializable {

	public enum CDMVersion {
		  CDMV4("CDMV4.csv")
		, CDMV50("CDMV5.0.csv")
		, CDMV51("CDMV5.1.csv")
		, CDMV52("CDMV5.2.csv")
		, CDMV53("CDMV5.3.csv")
		, CDMV54("CDMV5.4.csv")
		, CDMV60("CDMV6.0.csv")
		;

		private final String fileName;

		CDMVersion(String fileName) {
			this.fileName = fileName;
		}
	}

	private List<Table>			tables				= new ArrayList<Table>();
	private static final long	serialVersionUID	= -3912166654601191039L;
	private String				dbName				= "";
	private static final String	CONCEPT_ID_HINTS_FILE_NAME = "CDMConceptIDHints.csv";
	public String 				conceptIdHintsVocabularyVersion;

	public List<Table> getTables() {
		return tables;
	}

	public Table getTableByName(String name) {
		for (Table table : tables)
			if (table.getName().equalsIgnoreCase(name))
				return table;
		return null;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	public void addTable(Table table) {
		this.tables.add(table);
	}

	public String getDbName() {
		return dbName;
	}

	public static Database generateCDMModel(CDMVersion cdmVersion) {
		return Database.generateModelFromCSV(Database.class.getResourceAsStream(cdmVersion.fileName), cdmVersion.fileName);
	}

	public static Database generateModelFromCSV(InputStream stream, String dbName) {
		Database database = new Database();

		database.dbName = dbName.substring(0, dbName.lastIndexOf("."));

		Map<String, Table> nameToTable = new HashMap<>();
		try {
			ConceptsMap conceptIdHintsMap = new ConceptsMap(CONCEPT_ID_HINTS_FILE_NAME);
			database.conceptIdHintsVocabularyVersion = conceptIdHintsMap.vocabularyVersion;

			for (CSVRecord row : CSVFormat.RFC4180.withHeader().parse(new InputStreamReader(stream))) {
				String tableNameColumn;
				String fieldNameColumn;
				String isNullableColumn;
				String nullableValue;
				String dataTypeColumn;
				String descriptionColumn;
				if (row.isSet("TABLE_NAME")) {
					tableNameColumn = "TABLE_NAME";
					fieldNameColumn = "COLUMN_NAME";
					isNullableColumn = "IS_NULLABLE";
					nullableValue = "YES";
					dataTypeColumn = "DATA_TYPE";
					descriptionColumn = "DESCRIPTION";
				} else {
					tableNameColumn = "table";
					fieldNameColumn = "field";
					isNullableColumn = "required";
					nullableValue = "No";
					dataTypeColumn = "type";
					descriptionColumn = "description";
				}
				Table table = nameToTable.get(row.get(tableNameColumn).toLowerCase());

				if (table == null) {
					table = new Table();
					table.setDb(database);
					table.setName(row.get(tableNameColumn).toLowerCase());
					nameToTable.put(row.get(tableNameColumn).toLowerCase(), table);
					database.tables.add(table);
				}
				Field field = new Field(row.get(fieldNameColumn).toLowerCase(), table);
				field.setNullable(row.get(isNullableColumn).equals(nullableValue));
				field.setType(row.get(dataTypeColumn));
				field.setDescription(row.get(descriptionColumn));
				field.setConceptIdHints(conceptIdHintsMap.get(table.getName(), field.getName()));

				table.getFields().add(field);
			}
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
		return database;
	}

	public static Database generateModelFromScanReport(String filename) {
		return generateModelFromScanReport(filename, null);
	}

	/**
	 ** FieldName to lowerCase
	 */
	public static Database generateModelFromScanReport(String filename, String schemaName) {
		Database database = new Database();
		QuickAndDirtyXlsxReader workbook = new QuickAndDirtyXlsxReader(filename);

		// Create table lookup from tables overview, if it exists
		Map<String, Table> nameToTable = createTablesFromTableOverview(workbook, database, schemaName);

		// Field overview is the first sheet
		Sheet overviewSheet = workbook.getByName(ScanSheetName.FIELD_OVERVIEW);
		if (overviewSheet == null) {
			overviewSheet = workbook.get(0);
		}
		Iterator<QuickAndDirtyXlsxReader.Row> overviewRows = overviewSheet.iterator();

		overviewRows.next();  // Skip header
		while (overviewRows.hasNext()) {
			QuickAndDirtyXlsxReader.Row row = overviewRows.next();
			String tableName = row.getStringByHeaderName(ScanFieldName.TABLE);
			if (tableName.length() != 0) {
				if (schemaName != null) {
					tableName = String.format("%s.%s", schemaName, tableName);
				}
				// Get table created from table overview or created before
				Table table = nameToTable.get(tableName);

				// If not exists, create table from field overview sheet
				if (table == null) {
					table = createTable(
							tableName,
							"",
							row.getIntByHeaderName(ScanFieldName.N_ROWS),
							row.getIntByHeaderName(ScanFieldName.N_ROWS_CHECKED)
					);
					nameToTable.put(tableName, table);
					database.tables.add(table);
				}

				String fieldName = row.getStringByHeaderName(ScanFieldName.FIELD);
				Field field = new Field(fieldName.toLowerCase(), table);
				// Field field = new Field(fieldName, table);

				field.setType(row.getByHeaderName(ScanFieldName.TYPE));
				field.setMaxLength(row.getIntByHeaderName(ScanFieldName.MAX_LENGTH));
				field.setDescription(row.getStringByHeaderName(ScanFieldName.DESCRIPTION));
				field.setFractionEmpty(row.getDoubleByHeaderName(ScanFieldName.FRACTION_EMPTY));
				field.setUniqueCount(row.getIntByHeaderName(ScanFieldName.UNIQUE_COUNT));
				field.setFractionUnique(row.getDoubleByHeaderName(ScanFieldName.FRACTION_UNIQUE));
				field.setValueCounts(getValueCounts(workbook, tableName, fieldName));

				table.getFields().add(field);
			}
		}
		// database.defaultOrdering = new ArrayList<Table>(database.tables);
		return database;
	}

	/**
	 **  Deleted transform to lower case
	 */
	public static Table createTable(String name, String description, Integer nRows, Integer nRowsChecked) {
		Table table = new Table();
		// table.setName(name.toLowerCase());
		table.setName(name); // Original case
		table.setDescription(description);
		table.setRowCount(nRows == null ? -1 : nRows);
		table.setRowsCheckedCount(nRowsChecked == null ? -1 : nRowsChecked);
		return table;
	}

	/* Return map: key - table name; value - table object */
	public static Map<String, Table> createTablesFromTableOverview(QuickAndDirtyXlsxReader workbook,
																   Database database, String schemaName) {
		Sheet tableOverviewSheet = workbook.getByName(ScanSheetName.TABLE_OVERVIEW);

		if (tableOverviewSheet == null) { // No table overview sheet, empty nameToTable
			return new HashMap<>();
		}

		Map<String, Table> nameToTable = new HashMap<>();

		Iterator<org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Row> tableRows = tableOverviewSheet.iterator();
		tableRows.next();  // Skip header
		while (tableRows.hasNext()) {
			org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Row row = tableRows.next();
			String tableName = row.getByHeaderName(ScanFieldName.TABLE);
			if (schemaName != null) {
				tableName = String.format("%s.%s", schemaName, tableName);
			}
			Table table = createTable(
					tableName,
					row.getByHeaderName(ScanFieldName.DESCRIPTION),
					row.getIntByHeaderName(ScanFieldName.N_ROWS),
					row.getIntByHeaderName(ScanFieldName.N_ROWS_CHECKED)
			);
			// Add to lookup and database
			nameToTable.put(tableName, table);
			database.tables.add(table);
		}

		return nameToTable;
	}

	private static ValueCounts getValueCounts(QuickAndDirtyXlsxReader workbook, String tableName, String fieldName) {
		String targetSheetName = Table.createSheetNameFromTableName(tableName);
		int pointIndex = targetSheetName.indexOf('.'); // has schema name
		if (pointIndex != -1) {
			targetSheetName = targetSheetName.substring(pointIndex + 1);
		}
		Sheet tableSheet = workbook.getByName(targetSheetName);

		// Sheet not found for table, return empty
		if (tableSheet == null) {
			return new ValueCounts();
		}

		Iterator<org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Row> iterator = tableSheet.iterator();
		org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Row header = iterator.next();
		int index = header.indexOf(fieldName);

		ValueCounts valueCounts = new ValueCounts();
		if (index != -1) // Could happen when people manually delete columns
			while (iterator.hasNext()) {
				org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Row row = iterator.next();
				if (row.size() > index) {
					String value = row.get(index);
					String count;

					if (row.size() > index + 1) {
						count = row.get(index + 1);
					} else {
						count = "";
					}

					if (value.equals("") && count.equals("")) {
						break;
					}

					// If the count is not a number, ignore this row
					try {
						valueCounts.add(value, (int) Double.parseDouble(count));
					} catch (NumberFormatException e) {
						// Skip if count could not be parsed. In most cases this is for empty count at 'List Truncated...'
					}
				}
			}
		return valueCounts;
	}

}
