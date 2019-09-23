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
package org.ohdsi.whiteRabbit.scan;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.databases.RichConnection.QueryResult;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.CountingSet;
import org.ohdsi.utilities.collections.CountingSet.Count;
import org.ohdsi.utilities.collections.Pair;
import org.ohdsi.utilities.files.ReadTextFile;
import org.ohdsi.whiteRabbit.DbSettings;

public class SourceDataScan {

	public static int	MAX_VALUES_IN_MEMORY				= 100000;
	public static int	MIN_CELL_COUNT_FOR_CSV				= 1000000;
	public static int	N_FOR_FREE_TEXT_CHECK				= 1000;
	public static int	MIN_AVERAGE_LENGTH_FOR_FREE_TEXT	= 100;

	private char		delimiter							= ',';
	private int			sampleSize;
	private boolean		scanValues;
	private int			minCellCount;
	private int			maxValues;
	private DbType		dbType;
	private String		database;

	public void process(DbSettings dbSettings, int sampleSize, boolean scanValues, int minCellCount, int maxValues, String filename) {
		this.sampleSize = sampleSize;
		this.scanValues = scanValues;
		this.minCellCount = minCellCount;
		this.maxValues = maxValues;
		Map<String, List<FieldInfo>> tableToFieldInfos;
		if (dbSettings.dataType == DbSettings.CSVFILES) {
			if (!scanValues)
				this.minCellCount = Math.max(minCellCount, MIN_CELL_COUNT_FOR_CSV);
			tableToFieldInfos = processCsvFiles(dbSettings);
		} else
			tableToFieldInfos = processDatabase(dbSettings);
		generateReport(tableToFieldInfos, filename);
	}

	private Map<String, List<FieldInfo>> processDatabase(DbSettings dbSettings) {
		try (RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType)) {
			connection.setVerbose(false);
			connection.use(dbSettings.database);

			dbType = dbSettings.dbType;
			database = dbSettings.database;

			return dbSettings.tables.stream()
					.collect(Collectors.toMap(Function.identity(), table -> processDatabaseTable(table, connection)));

		}
	}

	private Map<String, List<FieldInfo>> processCsvFiles(DbSettings dbSettings) {
		delimiter = dbSettings.delimiter;
		Map<String, List<FieldInfo>> tableToFieldInfos = new HashMap<String, List<FieldInfo>>();
		for (String table : dbSettings.tables) {
			List<FieldInfo> fieldInfos = processCsvFile(table);
			String tableName = new File(table).getName();
			if (!tableToFieldInfos.containsKey(tableName)) {
				tableToFieldInfos.put(tableName, fieldInfos);
			} else {
				tableToFieldInfos.put(table, fieldInfos);
			}

		}
		return tableToFieldInfos;
	}

	private void generateReport(Map<String, List<FieldInfo>> tableToFieldInfos, String filename) {
		System.out.println("Generating scan report");
		removeEmptyTables(tableToFieldInfos);
		List<String> tables = new ArrayList<String>(tableToFieldInfos.keySet());
		Collections.sort(tables);

		SXSSFWorkbook workbook = new SXSSFWorkbook(100); // keep 100 rows in memory, exceeding rows will be flushed to disk
		CellStyle percentageStyle = workbook.createCellStyle();
		percentageStyle.setDataFormat(workbook.createDataFormat().getFormat("0%"));

		// Create overview sheet
		Sheet overviewSheet = workbook.createSheet("Overview");
		if (!scanValues) {
			addRow(overviewSheet, "Table", "Field", "Type", "N rows");
			for (String table : tables) {
				for (FieldInfo fieldInfo : tableToFieldInfos.get(table)) {
                    addRow(overviewSheet, table, fieldInfo.name, fieldInfo.getTypeDescription(), Long.valueOf(fieldInfo.rowCount));
                }
				addRow(overviewSheet, "");
			}
		} else {
			addRow(overviewSheet, "Table", "Field", "Type", "Max length", "N rows", "N rows checked", "Fraction empty", "N unique values", "Fraction unique values");
			int sheetIndex = 0;
			Map<String, String> sheetNameLookup = new HashMap<>();
			for (String tableName : tables) {
				// Make tablename unique
				String tableNameIndexed = Table.indexTableNameForSheet(tableName, sheetIndex);

				String sheetName = Table.createSheetNameFromTableName(tableNameIndexed);
				sheetNameLookup.put(tableName, sheetName);

				for (FieldInfo fieldInfo : tableToFieldInfos.get(tableName)) {
					Long uniqueCount = fieldInfo.uniqueCount;
					Double fractionUnique = fieldInfo.getFractionUnique();
                    addRow(overviewSheet, tableNameIndexed, fieldInfo.name, fieldInfo.getTypeDescription(),
							Integer.valueOf(fieldInfo.maxLength),
							Long.valueOf(fieldInfo.rowCount),
                            Long.valueOf(fieldInfo.nProcessed),
							fieldInfo.getFractionEmpty(),
							fieldInfo.hasValuesTrimmed() ? String.format("<= %d", uniqueCount) : uniqueCount,
							fieldInfo.hasValuesTrimmed() ? String.format("<= %.3f", fractionUnique) : fractionUnique
					);
					this.setCellStyles(overviewSheet, percentageStyle, 6, 8);
                }
				addRow(overviewSheet, "");
				sheetIndex += 1;
			}

			// Create per table scan values
			for (String tableName : tables) {
				Sheet valueSheet = workbook.createSheet(sheetNameLookup.get(tableName));

				List<FieldInfo> fieldInfos = tableToFieldInfos.get(tableName);
				List<List<Pair<String, Integer>>> valueCounts = new ArrayList<>();
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
				addRow(valueSheet, header);
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
					addRow(valueSheet, row);
				}
				// Save some memory by derefencing tables already included in the report:
				tableToFieldInfos.remove(tableName);
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
		tableToFieldInfos.entrySet()
				.removeIf(stringListEntry -> stringListEntry.getValue().size() == 0);
	}

	private List<FieldInfo> processDatabaseTable(String table, RichConnection connection) {
		StringUtilities.outputWithTime("Scanning table " + table);

		long rowCount = connection.getTableSize(table);
		List<FieldInfo> fieldInfos = fetchTableStructure(connection, table);
		if (scanValues) {
			int actualCount = 0;
			QueryResult queryResult = null;
			try {
				queryResult = fetchRowsFromTable(connection, table, rowCount);
				for (org.ohdsi.utilities.files.Row row : queryResult) {
					for (FieldInfo fieldInfo : fieldInfos) {
						fieldInfo.processValue(row.get(fieldInfo.name));
					}
					actualCount++;
					if (sampleSize != -1 && actualCount >= sampleSize) {
						System.out.println("Stopped after " + actualCount + " rows");
						break;
					}
				}
				for (FieldInfo fieldInfo : fieldInfos) {
					fieldInfo.trim();
				}
			} catch (Exception e) {
				System.out.println("Error: " + e.getMessage());
			} finally {
				if (queryResult != null) {
					queryResult.close();
				}
			}
		}

		return fieldInfos;
	}

	private QueryResult fetchRowsFromTable(RichConnection connection, String table, long rowCount) {
		String query = null;

		if (sampleSize == -1) {
			if (dbType == DbType.MSACCESS)
				query = "SELECT * FROM [" + table + "]";
			else if (dbType == DbType.MSSQL || dbType == DbType.PDW)
				query = "SELECT * FROM [" + table.replaceAll("\\.", "].[") + "]";
			else
				query = "SELECT * FROM " + table;
		} else {
			if (dbType == DbType.MSSQL)
				query = "SELECT * FROM [" + table.replaceAll("\\.", "].[") + "] TABLESAMPLE (" + sampleSize + " ROWS)";
			else if (dbType == DbType.MYSQL)
				query = "SELECT * FROM " + table + " ORDER BY RAND() LIMIT " + sampleSize;
			else if (dbType == DbType.PDW)
				query = "SELECT TOP " + sampleSize + " * FROM [" + table.replaceAll("\\.", "].[") + "] ORDER BY RAND()";
			else if (dbType == DbType.ORACLE) {
				if (sampleSize < rowCount) {
					double percentage = 100 * sampleSize / (double) rowCount;
					if (percentage < 100)
						query = "SELECT * FROM " + table + " SAMPLE(" + percentage + ")";
				} else {
					query = "SELECT * FROM " + table;
				}
			} else if (dbType == DbType.POSTGRESQL || dbType == DbType.REDSHIFT)
				query = "SELECT * FROM " + table + " ORDER BY RANDOM() LIMIT " + sampleSize;
			else if (dbType == DbType.MSACCESS)
				query = "SELECT " + "TOP " + sampleSize + " * FROM [" + table + "]";
		}
		// System.out.println("SQL: " + query);
		return connection.query(query);

	}

	private List<FieldInfo> fetchTableStructure(RichConnection connection, String table) {
		List<FieldInfo> fieldInfos = new ArrayList<FieldInfo>();

		if (dbType == DbType.MSACCESS) {
			ResultSet rs = connection.getMsAccessFieldNames(table);
			try {
				while (rs.next()) {
					FieldInfo fieldInfo = new FieldInfo(rs.getString("COLUMN_NAME"));
					fieldInfo.type = rs.getString("TYPE_NAME");
					fieldInfo.rowCount = connection.getTableSize(table);
					fieldInfos.add(fieldInfo);
				}
			} catch (SQLException e) {
				throw new RuntimeException(e.getMessage());
			}
		} else {
			String query = null;
			if (dbType == DbType.ORACLE)
				query = "SELECT COLUMN_NAME,DATA_TYPE FROM ALL_TAB_COLUMNS WHERE table_name = '" + table + "' AND owner = '" + database.toUpperCase() + "'";
			else if (dbType == DbType.MSSQL || dbType == DbType.PDW) {
				String trimmedDatabase = database;
				if (database.startsWith("[") && database.endsWith("]"))
					trimmedDatabase = database.substring(1, database.length() - 1);
				String[] parts = table.split("\\.");
				query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='" + trimmedDatabase + "' AND TABLE_SCHEMA='" + parts[0] +
						"' AND TABLE_NAME='" + parts[1]	+ "';";
			} else if (dbType == DbType.MYSQL)
				query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table
						+ "';";
			else if (dbType == DbType.POSTGRESQL || dbType == DbType.REDSHIFT)
				query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + database.toLowerCase() + "' AND TABLE_NAME = '"
						+ table.toLowerCase() + "' ORDER BY ordinal_position;";
			else if (dbType == DbType.TERADATA) {
				query = "SELECT ColumnName, ColumnType FROM dbc.columns WHERE DatabaseName= '" + database.toLowerCase() + "' AND TableName = '"
						+ table.toLowerCase() + "';";
			}

			for (org.ohdsi.utilities.files.Row row : connection.query(query)) {
				row.upperCaseFieldNames();
				FieldInfo fieldInfo;
				if (dbType == DbType.TERADATA) {
					fieldInfo = new FieldInfo(row.get("COLUMNNAME"));
				} else {
					fieldInfo = new FieldInfo(row.get("COLUMN_NAME"));
				}
				if (dbType == DbType.TERADATA) {
					fieldInfo.type = row.get("COLUMNTYPE");
				} else {
					fieldInfo.type = row.get("DATA_TYPE");
				}
				fieldInfo.rowCount = connection.getTableSize(table);
				;
				fieldInfos.add(fieldInfo);
			}
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
		public long					uniqueCount		= 0;
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
			if (valueCounts.size() > maxValues) {
				valueCounts.keepTopN(maxValues);
			}
		}

		public boolean hasValuesTrimmed() {
			return tooManyValues;
		}

		public Double getFractionEmpty() {
			if (nProcessed == 0)
				return 0d;
			else
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

		public Double getFractionUnique() {
			if (nProcessed == 0 || uniqueCount == 1) {
				return 0d;
			}
			else {
				return uniqueCount / (double) nProcessed;
			}

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
				boolean newlyAdded = valueCounts.add(value);
				if  (newlyAdded) uniqueCount++;

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
				valueCounts.addAll(StringUtilities.mapToWords(trimValue.toLowerCase()));
			}

			if (!tooManyValues && valueCounts.size() > MAX_VALUES_IN_MEMORY) {
				tooManyValues = true;
				this.trim();
			}
		}

		public List<Pair<String, Integer>> getSortedValuesWithoutSmallValues() {
			List<Pair<String, Integer>> result = valueCounts.key2count.entrySet().stream()
					.filter(e -> e.getValue().count >= minCellCount)
					.sorted(Comparator.<Map.Entry<String, Count>>comparingInt(e -> e.getValue().count).reversed())
					.limit(maxValues)
					.map(e -> new Pair<>(e.getKey(), e.getValue().count))
					.collect(Collectors.toCollection(ArrayList::new));

			if (result.size() < valueCounts.key2count.size()) {
				result.add(new Pair<>("List truncated...", -1));
			}
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

	private void setCellStyles(Sheet sheet, CellStyle style, int... colNums) {
		Row row = sheet.getRow(sheet.getLastRowNum());
		for(int i : colNums) {
			Cell cell = row.getCell(i);
			if (cell != null)
				cell.setCellStyle(style);
		}
	}
}
