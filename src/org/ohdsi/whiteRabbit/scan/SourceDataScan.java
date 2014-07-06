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
package org.ohdsi.whiteRabbit.scan;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.databases.RichConnection.QueryResult;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.CountingSet;
import org.ohdsi.utilities.collections.CountingSet.Count;
import org.ohdsi.utilities.collections.Pair;
import org.ohdsi.utilities.files.ReadTextFile;
import org.ohdsi.whiteRabbit.DbSettings;

public class SourceDataScan {
	
	public static int	MAX_VALUES_IN_MEMORY				= 100000;
	public static int	MAX_VALUES_TO_REPORT				= 25000;
	public static int	MIN_CELL_COUNT_FOR_CSV				= 1000000;
	public static int	N_FOR_FREE_TEXT_CHECK				= 1000;
	public static int	MIN_AVERAGE_LENGTH_FOR_FREE_TEXT	= 100;
	
	private char		delimiter							= ',';
	private int			sampleSize;
	private boolean		scanValues;
	private int			minCellCount;
	private DbType		dbType;
	private String		database;
	
	public static void main(String[] args) {
		// DbSettings dbSettings = new DbSettings();
		// dbSettings.dataType = DbSettings.DATABASE;
		// dbSettings.dbType = DbType.POSTGRESQL;
		// dbSettings.server = "127.0.0.1/test";
		// dbSettings.database = "test_schema";
		// dbSettings.tables.add("test_table");
		// dbSettings.user = "postgres";
		// dbSettings.password = "F1r3starter";
		// SourceDataScan scan = new SourceDataScan();
		// scan.process(dbSettings, 1000000, true, 25, "s:/data/ScanReport.xlsx");
		
		// DbSettings dbSettings = new DbSettings();
		// dbSettings.dataType = DbSettings.DATABASE;
		// dbSettings.dbType = DbType.ORACLE;
		// dbSettings.server = "127.0.0.1/xe";
		// dbSettings.database = "test";
		// dbSettings.tables.add("test_table");
		// dbSettings.user = "system";
		// dbSettings.password = "F1r3starter";
		// SourceDataScan scan = new SourceDataScan();
		// scan.process(dbSettings, 1000000, "s:/data/ScanReport.xlsx");
		
		DbSettings dbSettings = new DbSettings();
		dbSettings.dataType = DbSettings.DATABASE;
		dbSettings.dbType = DbType.MSSQL;
		dbSettings.server = "RNDUSRDHIT04";
		dbSettings.database = "[HCUP-NIS]";
		dbSettings.tables.add("hospital");
		dbSettings.tables.add("severity");
		dbSettings.tables.add("dx_pr_grps");
		dbSettings.tables.add("core");
		SourceDataScan scan = new SourceDataScan();
		scan.process(dbSettings, 1000000, true, 25, "s:/data/ScanReport.xlsx");
		
		// DbSettings dbSettings = new DbSettings();
		// dbSettings.dataType = DbSettings.DATABASE;
		// dbSettings.dbType = DbType.MYSQL;
		// dbSettings.server = "127.0.0.1";
		// dbSettings.database = "CDM_v4";
		// dbSettings.user = "root";
		// dbSettings.password = "F1r3starter";
		// dbSettings.tables.add("person");
		// dbSettings.tables.add("provider");
		// SourceDataScan scan = new SourceDataScan();
		// scan.process(dbSettings, 100000, true, 25, "c:/temp/ScanReport.xlsx");
		//
		// DbSettings dbSettings = new DbSettings();
		// dbSettings.dataType = DbSettings.CSVFILES;
		// dbSettings.delimiter = ',';
		// dbSettings.tables.add("S:/Data/ARS/Simulation/DDRUG.csv");
		// dbSettings.tables.add("S:/Data/ARS/Simulation/HOSP.csv");
		// SourceDataScan scan = new SourceDataScan();
		// scan.process(dbSettings, 100000, false, 25, "c:/temp/ScanReport.xlsx");
	}
	
	public void process(DbSettings dbSettings, int sampleSize, boolean scanValues, int minCellCount, String filename) {
		this.sampleSize = sampleSize;
		this.scanValues = scanValues;
		this.minCellCount = minCellCount;
		Map<String, List<FieldInfo>> tableToFieldInfos;
		if (dbSettings.dataType == DbSettings.CSVFILES) {
			if (!scanValues)
				minCellCount = Math.max(minCellCount, MIN_CELL_COUNT_FOR_CSV);
			tableToFieldInfos = processCsvFiles(dbSettings);
		} else
			tableToFieldInfos = processDatabase(dbSettings);
		generateReport(tableToFieldInfos, filename);
	}
	
	private Map<String, List<FieldInfo>> processDatabase(DbSettings dbSettings) {
		Map<String, List<FieldInfo>> tableToFieldInfos = new HashMap<String, List<FieldInfo>>();
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setVerbose(false);
		connection.use(dbSettings.database);
		
		dbType = dbSettings.dbType;
		database = dbSettings.database;
		
		for (String table : dbSettings.tables) {
			List<FieldInfo> fieldInfos = processDatabaseTable(table, connection);
			tableToFieldInfos.put(table, fieldInfos);
		}
		
		connection.close();
		return tableToFieldInfos;
	}
	
	private Map<String, List<FieldInfo>> processCsvFiles(DbSettings dbSettings) {
		delimiter = dbSettings.delimiter;
		Map<String, List<FieldInfo>> tableToFieldInfos = new HashMap<String, List<FieldInfo>>();
		for (String table : dbSettings.tables) {
			List<FieldInfo> fieldInfos = processCsvFile(table);
			String tableName = new File(table).getName();
			tableToFieldInfos.put(tableName, fieldInfos);
		}
		return tableToFieldInfos;
	}
	
	private void generateReport(Map<String, List<FieldInfo>> tableToFieldInfos, String filename) {
		System.out.println("Generating scan report");
		removeEmptyTables(tableToFieldInfos);
		List<String> tables = new ArrayList<String>(tableToFieldInfos.keySet());
		Collections.sort(tables);
		
		SXSSFWorkbook workbook = new SXSSFWorkbook(100); // keep 100 rows in memory, exceeding rows will be flushed to disk
		
		// Create overview sheet
		Sheet sheet = workbook.createSheet("Overview");
		if (!scanValues) {
			addRow(sheet, "Table", "Field", "Type", "N rows");
			for (String table : tables) {
				for (FieldInfo fieldInfo : tableToFieldInfos.get(table))
					addRow(sheet, table, fieldInfo.name, fieldInfo.getTypeDescription(), Long.valueOf(fieldInfo.rowCount));
				addRow(sheet, "");
			}
		} else {
			addRow(sheet, "Table", "Field", "Type", "Max length", "N rows", "N rows checked", "Fraction empty");
			for (String table : tables) {
				for (FieldInfo fieldInfo : tableToFieldInfos.get(table))
					addRow(sheet, table, fieldInfo.name, fieldInfo.getTypeDescription(), Integer.valueOf(fieldInfo.maxLength),
							Long.valueOf(fieldInfo.rowCount), Long.valueOf(fieldInfo.nProcessed), fieldInfo.getFractionEmpty());
				addRow(sheet, "");
			}
			
			// Create per table sheets
			for (String table : tables) {
				sheet = workbook.createSheet(table);
				List<FieldInfo> fieldInfos = tableToFieldInfos.get(table);
				List<List<Pair<String, Integer>>> valueCounts = new ArrayList<List<Pair<String, Integer>>>();
				Object[] header = new Object[fieldInfos.size() * 2];
				int maxCount = 0;
				for (int i = 0; i < fieldInfos.size(); i++) {
					FieldInfo fieldInfo = fieldInfos.get(i);
					header[i * 2] = fieldInfo.name;
					if (fieldInfo.isFreeText)
						header[(i * 2) + 1] = "Word count";
					else
						header[(i * 2) + 1] = "Frequency";
					List<Pair<String, Integer>> counts = fieldInfo.getSortedValuesWithoutSmallValues();
					valueCounts.add(counts);
					if (counts.size() > maxCount)
						maxCount = counts.size();
				}
				addRow(sheet, header);
				for (int i = 0; i < maxCount; i++) {
					Object[] row = new Object[fieldInfos.size() * 2];
					for (int j = 0; j < fieldInfos.size(); j++) {
						List<Pair<String, Integer>> counts = valueCounts.get(j);
						if (counts.size() > i) {
							row[j * 2] = counts.get(i).getItem1();
							row[(j * 2) + 1] = counts.get(i).getItem2() == -1 ? "" : counts.get(i).getItem2();
						} else {
							row[j * 2] = "";
							row[(j * 2) + 1] = "";
						}
					}
					addRow(sheet, row);
				}
				// Save some memory by derefencing tables already included in the report:
				tableToFieldInfos.remove(table);
			}
		}
		
		try {
			FileOutputStream out = new FileOutputStream(new File(filename));
			workbook.write(out);
			out.close();
			StringUtilities.outputWithTime("Scan report generated: " + filename);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	private void removeEmptyTables(Map<String, List<FieldInfo>> tableToFieldInfos) {
		Iterator<Map.Entry<String, List<FieldInfo>>> iterator = tableToFieldInfos.entrySet().iterator();
		while (iterator.hasNext()) {
			if (iterator.next().getValue().size() == 0)
				iterator.remove();
		}
	}
	
	private List<FieldInfo> processDatabaseTable(String table, RichConnection connection) {
		StringUtilities.outputWithTime("Scanning table " + table);
		
		long rowCount = connection.getTableSize(table);
		if (rowCount == 0)
			return new ArrayList<SourceDataScan.FieldInfo>();
		
		List<FieldInfo> fieldInfos = fetchTableStructure(connection, rowCount, table);
		
		if (scanValues) {
			int actualCount = 0;
			QueryResult queryResult = fetchRowsFromTable(connection, table, rowCount);
			for (org.ohdsi.utilities.files.Row row : queryResult) {
				for (int i = 0; i < fieldInfos.size(); i++)
					fieldInfos.get(i).processValue(row.getCells().get(i));
				actualCount++;
				if (sampleSize != -1 && actualCount >= sampleSize) {
					System.out.println("Stopped after " + actualCount + " rows");
					break;
				}
			}
			queryResult.close(); // Not normally needed, but if we ended prematurely make sure its closed
			for (FieldInfo fieldInfo : fieldInfos)
				fieldInfo.trim();
		}
		
		return fieldInfos;
	}
	
	private QueryResult fetchRowsFromTable(RichConnection connection, String table, long rowCount) {
		String query;
		if (dbType == DbType.MSSQL)
			query = "SELECT * FROM [" + table + "]";
		else
			query = "SELECT * FROM " + table;
		
		if (sampleSize != -1) {
			if (dbType == DbType.MSSQL)
				query += " TABLESAMPLE (" + sampleSize + " ROWS)";
			else if (dbType == DbType.MYSQL)
				query += " ORDER BY RAND() LIMIT " + sampleSize;
			else if (dbType == DbType.ORACLE) {
				if (sampleSize < rowCount) {
					double percentage = 100 * sampleSize / (double) rowCount;
					if (percentage < 100)
						query += " SAMPLE(" + percentage + ")";
				}
			} else if (dbType == DbType.POSTGRESQL)
				query += " ORDER BY RANDOM() LIMIT " + sampleSize;
		}
		// System.out.println("SQL: " + query);
		return connection.query(query);
		
	}
	
	private List<FieldInfo> fetchTableStructure(RichConnection connection, long rowCount, String table) {
		String query = null;
		if (dbType == DbType.ORACLE)
			query = "SELECT COLUMN_NAME,DATA_TYPE FROM ALL_TAB_COLUMNS WHERE table_name = '" + table + "' AND owner = '" + database.toUpperCase() + "'";
		else if (dbType == DbType.MSSQL) {
			String trimmedDatabase = database;
			if (database.startsWith("[") && database.endsWith("]"))
				trimmedDatabase = database.substring(1, database.length() - 1);
			query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='" + trimmedDatabase + "' AND TABLE_NAME='" + table
					+ "';";
		} else if (dbType == DbType.MYSQL)
			query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table + "';";
		else if (dbType == DbType.POSTGRESQL)
			query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + database.toLowerCase() + "' AND TABLE_NAME = '"
					+ table.toLowerCase() + "' ORDER BY ordinal_position;";
		
		List<FieldInfo> fieldInfos = new ArrayList<FieldInfo>();
		for (org.ohdsi.utilities.files.Row row : connection.query(query)) {
			row.upperCaseFieldNames();
			FieldInfo fieldInfo = new FieldInfo(row.get("COLUMN_NAME"));
			fieldInfo.type = row.get("DATA_TYPE");
			fieldInfo.rowCount = rowCount;
			fieldInfos.add(fieldInfo);
		}
		return fieldInfos;
	}
	
	private List<FieldInfo> processCsvFile(String filename) {
		StringUtilities.outputWithTime("Scanning table " + filename);
		List<FieldInfo> fieldInfos = new ArrayList<FieldInfo>();
		int lineNr = 0;
		for (String line : new ReadTextFile(filename)) {
			lineNr++;
			List<String> row = StringUtilities.safeSplit(line, delimiter);
			for (int i = 0; i < row.size(); i++) {
				String column = row.get(i);
				if (column.startsWith("\"") && column.endsWith("\"") && column.length() > 1)
					column = column.substring(1, column.length() - 1);
				column = column.replace("\\\"", "\"");
				row.set(i, column);
			}
			if (lineNr == 1) {
				for (String cell : row)
					fieldInfos.add(new FieldInfo(cell));
			} else {
				if (row.size() == fieldInfos.size()) { // Else there appears to be a formatting error, so skip
					for (int i = 0; i < row.size(); i++)
						fieldInfos.get(i).processValue(row.get(i));
				}
			}
			if (sampleSize != -1 && lineNr == sampleSize)
				break;
		}
		for (FieldInfo fieldInfo : fieldInfos)
			fieldInfo.trim();
		
		return fieldInfos;
	}
	
	private class FieldInfo {
		public String				type;
		public String				name;
		public CountingSet<String>	valueCounts		= new CountingSet<String>();
		public long					sumLength		= 0;
		public int					maxLength		= 0;
		public long					nProcessed		= 0;
		public long					emptyCount		= 0;
		public long					rowCount		= -1;
		public boolean				isInteger		= true;
		public boolean				isReal			= true;
		public boolean				isDate			= true;
		public boolean				isFreeText		= false;
		public boolean				tooManyValues	= false;
		
		public FieldInfo(String name) {
			this.name = name;
		}
		
		public void trim() {
			if (valueCounts.size() > MAX_VALUES_TO_REPORT)
				valueCounts.keepTopN(MAX_VALUES_TO_REPORT);
		}
		
		public Double getFractionEmpty() {
			return emptyCount / (double) nProcessed;
		}
		
		public String getTypeDescription() {
			if (type != null)
				return type;
			else if (nProcessed == emptyCount)
				return "empty";
			else if (isFreeText)
				return "text";
			else if (isDate)
				return "date";
			else if (isInteger)
				return "int";
			else if (isReal)
				return "real";
			else
				return "varchar";
		}
		
		public void processValue(String value) {
			String trimValue = value.trim();
			nProcessed++;
			sumLength += value.length();
			if (value.length() > maxLength)
				maxLength = value.length();
			
			if (trimValue.length() == 0)
				emptyCount++;
			
			if (!isFreeText) {
				valueCounts.add(value);
				
				if (trimValue.length() != 0) {
					if (isReal && !StringUtilities.isNumber(trimValue))
						isReal = false;
					if (isInteger && !StringUtilities.isLong(trimValue))
						isInteger = false;
					if (isDate && !StringUtilities.isDate(trimValue))
						isDate = false;
				}
				if (nProcessed == N_FOR_FREE_TEXT_CHECK) {
					if (!isInteger && !isReal && !isDate) {
						double averageLength = sumLength / (double) (nProcessed - emptyCount);
						if (averageLength >= MIN_AVERAGE_LENGTH_FOR_FREE_TEXT) {
							isFreeText = true;
							CountingSet<String> wordCounts = new CountingSet<String>();
							for (Map.Entry<String, Count> entry : valueCounts.key2count.entrySet())
								for (String word : StringUtilities.mapToWords(entry.getKey().toLowerCase()))
									wordCounts.add(word, entry.getValue().count);
							valueCounts = wordCounts;
						}
					}
				}
			} else {
				for (String word : StringUtilities.mapToWords(trimValue.toLowerCase()))
					valueCounts.add(word);
			}
			
			if (!tooManyValues && valueCounts.size() > MAX_VALUES_IN_MEMORY) {
				tooManyValues = true;
				valueCounts.keepTopN(MAX_VALUES_TO_REPORT);
			}
		}
		
		public List<Pair<String, Integer>> getSortedValuesWithoutSmallValues() {
			boolean truncated = false;
			List<Pair<String, Integer>> result = new ArrayList<Pair<String, Integer>>();
			
			for (Map.Entry<String, Count> entry : valueCounts.key2count.entrySet()) {
				if (entry.getValue().count < minCellCount)
					truncated = true;
				else {
					result.add(new Pair<String, Integer>(entry.getKey(), entry.getValue().count));
					if (result.size() > MAX_VALUES_TO_REPORT) {
						truncated = true;
						break;
					}
				}
			}
			
			Collections.sort(result, new Comparator<Pair<String, Integer>>() {
				public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
					return o2.getItem2().compareTo(o1.getItem2());
				}
			});
			if (truncated)
				result.add(new Pair<String, Integer>("List truncated...", -1));
			return result;
		}
	}
	
	private void addRow(Sheet sheet, Object... values) {
		Row row = sheet.createRow(sheet.getPhysicalNumberOfRows());
		for (Object value : values) {
			Cell cell = row.createCell(row.getPhysicalNumberOfCells());
			
			if (value instanceof Integer || value instanceof Long || value instanceof Double)
				cell.setCellValue(Double.parseDouble(value.toString()));
			else
				cell.setCellValue(value.toString());
			
		}
	}
}
