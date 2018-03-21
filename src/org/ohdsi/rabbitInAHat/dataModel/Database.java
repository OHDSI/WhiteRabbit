/*******************************************************************************
 * Copyright 2017 Observational Health Data Sciences and Informatics
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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.ohdsi.utilities.files.QuickAndDirtyXlsxReader;
import org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Sheet;

public class Database implements Serializable {
	
	private List<Table>			tables				= new ArrayList<Table>();
	private static final long	serialVersionUID	= -3912166654601191039L;
	private String				dbName				= "";

	public Database() {
	}
	
	public Database(Database database) {
		tables = new ArrayList<Table>(database.tables);
		dbName = database.dbName;
	}

	public enum CDMVersion {
		CDMV4("CDMV4.csv"), CDMV5("CDMV5.csv"), CDMV501("CDMV5.0.1.csv"), CDMV510("CDMV5.1.0.csv"), CDMV520("CDMV5.2.0.csv");

		private final String	fileName;

		CDMVersion(String fileName) {
			this.fileName = fileName;
		}
	}

	public List<Table> getTables() {
		return tables;
	}

	public Table getTableByName(String name) {
		for (Table table : tables)
			if (table.getName().toLowerCase().equals(name.toLowerCase()))
				return table;
		return null;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
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

		Map<String, Table> nameToTable = new HashMap<String, Table>();
		try {
			for (CSVRecord row : CSVFormat.RFC4180.withHeader().parse(new InputStreamReader(stream))) {

				Table table = nameToTable.get(row.get("TABLE_NAME").toLowerCase());

				if (table == null) {
					table = new Table();
					table.setDb(database);
					table.setName(row.get("TABLE_NAME").toLowerCase());
					nameToTable.put(row.get("TABLE_NAME").toLowerCase(), table);
					database.tables.add(table);
				}
				Field field = new Field(row.get("COLUMN_NAME").toLowerCase(), table);
				field.setNullable(row.get("IS_NULLABLE").equals("YES"));
				field.setType(row.get("DATA_TYPE"));
				field.setDescription(row.get("DESCRIPTION"));
				table.getFields().add(field);
			}
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
		return database;
	}

	public static Database generateModelFromScanReport(String filename) {
		Database database = new Database();
		Map<String, Table> nameToTable = new HashMap<String, Table>();
		QuickAndDirtyXlsxReader workbook = new QuickAndDirtyXlsxReader(filename);
		Sheet sheet = workbook.get(0);
		Iterator<org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Row> iterator = sheet.iterator();
		Map<String, Integer> fieldName2ColumnIndex = new HashMap<String, Integer>();
		for (String header : iterator.next())
			fieldName2ColumnIndex.put(header, fieldName2ColumnIndex.size());

		while (iterator.hasNext()) {
			org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Row row = iterator.next();
			String tableName = row.get(fieldName2ColumnIndex.get("Table"));
			if (tableName.length() != 0) {
				Table table = nameToTable.get(tableName);
				if (table == null) {
					table = new Table();
					table.setName(tableName.toLowerCase());
					table.setRowCount((int) Double.parseDouble(row.get(fieldName2ColumnIndex.get("N rows"))));
					nameToTable.put(tableName, table);
					database.tables.add(table);
				}
				String fieldName = row.get(fieldName2ColumnIndex.get("Field"));
				Field field = new Field(fieldName.toLowerCase(), table);
				Integer index;
				// Someone may have manually deleted data, so can't assume this
				// is always there:
				index = fieldName2ColumnIndex.get("Fraction empty");
				if (index != null && index < row.size())
					field.setNullable(!row.get(index).equals("0"));

				index = fieldName2ColumnIndex.get("Type");
				if (index != null && index < row.size())
					field.setType(row.get(index));

				index = fieldName2ColumnIndex.get("Max length");
				if (index != null && index >= 0 && index < row.size())
					field.setMaxLength((int) (Double.parseDouble(row.get(index))));
				field.setValueCounts(getValueCounts(workbook, tableName, fieldName));
				table.getFields().add(field);
			}
		}
		// database.defaultOrdering = new ArrayList<Table>(database.tables);
		return database;
	}

	private static String[][] getValueCounts(QuickAndDirtyXlsxReader workbook, String tableName, String fieldName) {
		Sheet tableSheet = null;
		for (Sheet sheet : workbook)
			if (sheet.getName().equals(tableName)) {
				tableSheet = sheet;
				break;
			}
		if (tableSheet == null) // Sheet not found for table, return empty array
			return new String[0][0];

		Iterator<org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Row> iterator = tableSheet.iterator();
		org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Row header = iterator.next();
		int index = header.indexOf(fieldName);
		List<String[]> list = new ArrayList<String[]>();
		if (index != -1) // Could happen when people manually delete columns
			while (iterator.hasNext()) {
				org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Row row = iterator.next();
				if (row.size() > index) {
					String value = row.get(index);
					String count;
					if (row.size() > index + 1)
						count = row.get(index + 1);
					else
						count = "";
					if (value.equals("") && count.equals(""))
						break;
					list.add(new String[] { value, count });
				}
			}
		return list.toArray(new String[list.size()][2]);
	}

}
