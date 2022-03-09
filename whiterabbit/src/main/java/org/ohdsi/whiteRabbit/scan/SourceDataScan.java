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

import com.epam.parso.Column;
import com.epam.parso.SasFileProperties;
import com.epam.parso.SasFileReader;
import com.epam.parso.impl.SasFileReaderImpl;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.databases.RichConnection.QueryResult;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.*;
import org.ohdsi.utilities.collections.CountingSet;
import org.ohdsi.utilities.collections.CountingSet.Count;
import org.ohdsi.utilities.collections.Pair;
import org.ohdsi.utilities.files.ReadTextFile;
import org.ohdsi.whiteRabbit.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Long.max;
import static org.ohdsi.whiteRabbit.scan.SchemaUtil.adaptSchemaNameForPostgres;

public class SourceDataScan {

	public static int	MAX_VALUES_IN_MEMORY				= 100000;
	public static int	MIN_CELL_COUNT_FOR_CSV				= 1000000;
	public static int	N_FOR_FREE_TEXT_CHECK				= 1000;
	public static int	MIN_AVERAGE_LENGTH_FOR_FREE_TEXT	= 100;

	private SXSSFWorkbook workbook;
	private char delimiter = ',';
	private int sampleSize;
	private boolean scanValues = false;
	private boolean calculateNumericStats = false;
	private int numStatsSamplerSize;
	private int minCellCount; // minimum frequency required to add a value to the report
	private int maxValues; // maximum number of values in the report
	private DbSettings.SourceType sourceType;
	private DbType dbType;
	private String database;
	private Map<Table, List<FieldInfo>> tableToFieldInfos;
	private Map<String, String> indexedTableNameLookup;
	private boolean isFile = true;

	private LocalDateTime startTimeStamp;

	private Logger logger = new ConsoleLogger();
	private Interrupter interrupter = new ThreadInterrupter();

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public void setInterrupter(Interrupter interrupter) {
		this.interrupter = interrupter;
	}

	public void setSampleSize(int sampleSize) {
		// -1 if sample size is not restricted
		this.sampleSize = sampleSize;
	}

	public void setScanValues(boolean scanValues) {
		this.scanValues = scanValues;
	}

	public void setMinCellCount(int minCellCount) {
		this.minCellCount = minCellCount;
	}

	public void setMaxValues(int maxValues) {
		this.maxValues = maxValues;
	}

	public void setCalculateNumericStats(boolean calculateNumericStats) {
		this.calculateNumericStats = calculateNumericStats;
	}

	public void setNumStatsSamplerSize(int numStatsSamplerSize) {
		this.numStatsSamplerSize = numStatsSamplerSize;
	}

	public void process(DbSettings dbSettings, String outputFileName) throws InterruptedException {
		startTimeStamp = LocalDateTime.now();
		sourceType = dbSettings.sourceType;
		dbType = dbSettings.dbType;
		database = dbSettings.database;

		tableToFieldInfos = new HashMap<>();
		int tablesCount = dbSettings.tables.size();
		logger.setItemsCount(tablesCount);
		logger.info("Started new scan of " + dbSettings.tables.size() + " tables...");
		if (sourceType == DbSettings.SourceType.CSV_FILES) {
			if (!scanValues)
				this.minCellCount = Math.max(minCellCount, MIN_CELL_COUNT_FOR_CSV);
			processCsvFiles(dbSettings);
		} else if (sourceType == DbSettings.SourceType.SAS_FILES) {
			throw new UnsupportedOperationException("SAS files does not support");
			// after implementing support must configure logging
			// processSasFiles(dbSettings);
		} else {
			isFile = false;
			processDatabase(dbSettings);
		}

		generateReport(outputFileName);
	}

	private void processDatabase(DbSettings dbSettings) throws InterruptedException {
		// GBQ requires database. Put database value into domain var
		if (dbSettings.dbType == DbType.BIGQUERY) {
			dbSettings.domain = dbSettings.database;
		}

		try (RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType)) {
			connection.setVerbose(false);
			connection.use(adaptSchemaNameForPostgres(dbSettings, dbSettings.database));

			for (String table : dbSettings.tables) {
				interrupter.checkWasInterrupted();
				tableToFieldInfos.put(new Table(table), processDatabaseTable(table, connection));
				logger.incrementScannedItems();
				logger.info("Scanned table " + table);
			}
		}
	}

	private void processCsvFiles(DbSettings dbSettings) throws InterruptedException {
		delimiter = dbSettings.delimiter;
		for (String fileName : dbSettings.tables) {
			interrupter.checkWasInterrupted();
			Table table = new Table();
			table.setName(new File(fileName).getName());
			List<FieldInfo> fieldInfos = processCsvFile(fileName);
			tableToFieldInfos.put(table, fieldInfos);
			logger.incrementScannedItems();
			logger.info("Scanned file " + StringUtilities.getFileNameBYFullName(fileName));
		}
	}

	@Deprecated
	private void processSasFiles(DbSettings dbSettings) throws InterruptedException {
		for (String fileName : dbSettings.tables) {
			try(FileInputStream inputStream = new FileInputStream(new File(fileName))) {
				SasFileReader sasFileReader = new SasFileReaderImpl(inputStream);
				SasFileProperties sasFileProperties = sasFileReader.getSasFileProperties();

				Table table = new Table(new File(fileName).getName());
				table.setName(new File(fileName).getName());
				table.setComment(sasFileProperties.getName());

				logger.info("Scanning table " + StringUtilities.getFileNameBYFullName(fileName));
				List<FieldInfo> fieldInfos = processSasFile(sasFileReader);
				tableToFieldInfos.put(table, fieldInfos);

			} catch (IOException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private void generateReport(String filename) {
		logger.info("Generating scan report");
		removeEmptyTables();

		workbook = new SXSSFWorkbook(100); // keep 100 rows in memory, exceeding rows will be flushed to disk

		int i = 0;
		indexedTableNameLookup = new HashMap<>();
		for (Table table : tableToFieldInfos.keySet()) {
			String tableNameIndexed = Table.indexTableNameForSheetAndRemoveSchema(table.getName(), i, isFile);
			indexedTableNameLookup.put(table.getName(), tableNameIndexed);
			i++;
		}

		createFieldOverviewSheet();
		createTableOverviewSheet();

		if (scanValues) {
			createValueSheet();
		}

		createMetaSheet();

		try (FileOutputStream out = new FileOutputStream(new File(filename))) {
			workbook.write(out);
			out.close();
			logger.info("Scan report generated");
		} catch (IOException ex) {
			logger.error(ex.getMessage());
			throw new RuntimeException(ex.getMessage());
		}
	}

	private void createFieldOverviewSheet() {
		Sheet overviewSheet = workbook.createSheet(ScanSheetName.FIELD_OVERVIEW);
		CellStyle percentageStyle = workbook.createCellStyle();
		percentageStyle.setDataFormat(workbook.createDataFormat().getFormat("0.0%"));

		// Create heading
		List<String> overviewHeader = new ArrayList<>(Arrays.asList(
				ScanFieldName.TABLE,
				ScanFieldName.FIELD,
				ScanFieldName.DESCRIPTION,
				ScanFieldName.TYPE,
				ScanFieldName.MAX_LENGTH,
				ScanFieldName.N_ROWS
		));
		if (scanValues) {
			overviewHeader.addAll(Arrays.asList(
					ScanFieldName.N_ROWS_CHECKED,
					ScanFieldName.FRACTION_EMPTY,
					ScanFieldName.UNIQUE_COUNT,
					ScanFieldName.FRACTION_UNIQUE
			));
			if (calculateNumericStats) {
				overviewHeader.addAll(Arrays.asList(
						ScanFieldName.AVERAGE,
						ScanFieldName.STDEV,
						ScanFieldName.MIN,
						ScanFieldName.Q1,
						ScanFieldName.Q2,
						ScanFieldName.Q3,
						ScanFieldName.MAX
				));
			}
		}
		addRow(overviewSheet, overviewHeader.toArray());

		// Add fields
		for (Table table : tableToFieldInfos.keySet()) {
			String tableName = table.getName();
			String tableNameIndexed = indexedTableNameLookup.get(tableName);

			for (FieldInfo fieldInfo : tableToFieldInfos.get(table)) {
				List<Object> values = new ArrayList<>(Arrays.asList(
						tableNameIndexed,
						fieldInfo.name,
						fieldInfo.label,
						fieldInfo.getTypeDescription(),
						fieldInfo.maxLength,
						fieldInfo.rowCount
				));

				if (scanValues) {
					Long uniqueCount = fieldInfo.uniqueCount;
					Double fractionUnique = fieldInfo.getFractionUnique();
					values.addAll(Arrays.asList(
							fieldInfo.nProcessed,
							fieldInfo.getFractionEmpty(),
							fieldInfo.hasValuesTrimmed() ? String.format("<= %d", uniqueCount) : uniqueCount,
							fieldInfo.hasValuesTrimmed() ? String.format("<= %.3f", fractionUnique) : fractionUnique
					));
					if (calculateNumericStats) {
						values.addAll(Arrays.asList(
								fieldInfo.average,
								fieldInfo.stdev,
								fieldInfo.minimum,
								fieldInfo.q1,
								fieldInfo.q2,
								fieldInfo.q3,
								fieldInfo.maximum
						));
					}
				}
				Row row = addRow(overviewSheet, values.toArray());
				if (scanValues) {
					setColumnStyles(row, percentageStyle, 7, 9);
				}
			}
			addRow(overviewSheet, "");
		}
	}

	private void createTableOverviewSheet() {
		Sheet tableOverviewSheet = workbook.createSheet(ScanSheetName.TABLE_OVERVIEW);

		addRow(tableOverviewSheet,
				ScanFieldName.TABLE,
				ScanFieldName.DESCRIPTION,
				ScanFieldName.N_ROWS,
				ScanFieldName.N_ROWS_CHECKED,
				ScanFieldName.N_FIELDS,
				ScanFieldName.N_FIELDS_EMPTY
		);

		for (Table table : tableToFieldInfos.keySet()) {
			String tableName = table.getName();
			String tableNameIndexed = indexedTableNameLookup.get(tableName);
			String description = table.getComment();
			long rowCount = -1;
			long rowCheckedCount = -1;
			long nFields = 0;
			long nFieldsEmpty = 0;
			for (FieldInfo fieldInfo : tableToFieldInfos.get(table)) {
				rowCount = max(rowCount, fieldInfo.rowCount);
				rowCheckedCount = max(rowCheckedCount, fieldInfo.nProcessed);
				nFields += 1;
				if (scanValues) {
					nFieldsEmpty += fieldInfo.getFractionEmpty() == 1 ? 1 : 0;
				}
			}
			addRow(tableOverviewSheet,
					tableNameIndexed,
					description,
					rowCount,
					rowCheckedCount,
					nFields,
					scanValues ? nFieldsEmpty : -1
			);
		}
	}

	private void createValueSheet() {
		// Make a copy of the tableNames, such that we can dereference the table at the end of each loop to save memory
		Table[] tables = tableToFieldInfos.keySet().toArray(new Table[0]);

		for (Table table : tables) {
			String tableName = table.getName();
			String tableNameIndexed = indexedTableNameLookup.get(tableName);
			Sheet valueSheet = workbook.createSheet(Table.createSheetNameFromTableName(tableNameIndexed));

			List<FieldInfo> fieldInfos = tableToFieldInfos.get(table);
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
			// Save some memory by dereferencing tables already included in the report:
			tableToFieldInfos.remove(table);
		}
	}

	private void createMetaSheet() {
		// All variables to be stored
		Sheet metaSheet = workbook.createSheet("_");
		addRow(metaSheet, "Key", "Value");
		addRow(metaSheet, "Version", Version.getVersion(this.getClass()));
		addRow(metaSheet, "Scan started at ", startTimeStamp.toString());
		addRow(metaSheet, "Scan finished at", LocalDateTime.now().toString());
		addRow(metaSheet, "MAX_VALUES_IN_MEMORY", SourceDataScan.MAX_VALUES_IN_MEMORY);
		addRow(metaSheet, "MIN_CELL_COUNT_FOR_CSV", SourceDataScan.MIN_CELL_COUNT_FOR_CSV);
		addRow(metaSheet, "N_FOR_FREE_TEXT_CHECK", SourceDataScan.N_FOR_FREE_TEXT_CHECK);
		addRow(metaSheet, "MIN_AVERAGE_LENGTH_FOR_FREE_TEXT", SourceDataScan.MIN_AVERAGE_LENGTH_FOR_FREE_TEXT);
		addRow(metaSheet, "sourceType", this.sourceType.toString());
		addRow(metaSheet, "dbType", this.dbType != null ? this.dbType.getTypeName() : "");
//		addRow(metaSheet, "database", this.database);
		addRow(metaSheet, "delimiter", this.delimiter);
		addRow(metaSheet, "sampleSize", this.sampleSize);
		addRow(metaSheet, "scanValues", this.scanValues);
		addRow(metaSheet, "minCellCount", this.minCellCount);
		addRow(metaSheet, "maxValues", this.maxValues);
		addRow(metaSheet, "calculateNumericStats", this.calculateNumericStats);
		addRow(metaSheet, "numStatsSamplerSize", this.numStatsSamplerSize);

	}

	private void removeEmptyTables() {
		tableToFieldInfos.entrySet()
				.removeIf(stringListEntry -> stringListEntry.getValue().size() == 0);
	}

	private List<FieldInfo> processDatabaseTable(String table, RichConnection connection) {
		logger.info("Scanning table " + table);

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
						logger.warning("Stopped after " + actualCount + " rows");
						break;
					}
				}
				for (FieldInfo fieldInfo : fieldInfos)
					fieldInfo.trim();
			} catch (Exception e) {
				logger.error("Error: " + e.getMessage());
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
			else if (dbType == DbType.MSSQL || dbType == DbType.PDW || dbType == DbType.AZURE)
				query = "SELECT * FROM [" + table.replaceAll("\\.", "].[") + "]";
			else
				query = "SELECT * FROM " + table;
		} else {
			if (dbType == DbType.MSSQL || dbType == DbType.AZURE)
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
			else if (dbType == DbType.BIGQUERY)
				query = "SELECT * FROM " + table + " ORDER BY RAND() LIMIT " + sampleSize;
		}
		// logger.log("SQL: " + query);
		return connection.query(query);

	}

	private List<FieldInfo> fetchTableStructure(RichConnection connection, String table) {
		List<FieldInfo> fieldInfos = new ArrayList<>();

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
			} else if (dbType == DbType.AZURE) {
				String[] parts = table.split("\\.");
				query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + parts[0] +
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
			else if (dbType == DbType.BIGQUERY) {
				query = "SELECT column_name AS COLUMN_NAME, data_type as DATA_TYPE FROM " + database + ".INFORMATION_SCHEMA.COLUMNS WHERE table_name = \"" + table + "\";";
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
				fieldInfos.add(fieldInfo);
			}
		}
		return fieldInfos;
	}

	private List<FieldInfo> processCsvFile(String filename) {
		logger.info("Scanning file " + StringUtilities.getFileNameBYFullName(filename));
		List<FieldInfo> fieldInfos = new ArrayList<>();
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
				for (String cell : row) {
					fieldInfos.add(new FieldInfo(cell));
				}

				if (!scanValues) {
					return fieldInfos;
				}
			} else {
				if (row.size() == fieldInfos.size()) { // Else there appears to be a formatting error, so skip
					for (int i = 0; i < row.size(); i++) {
						fieldInfos.get(i).processValue(row.get(i));
					}
				}
			}
			if (sampleSize != -1 && lineNr > sampleSize)
				break;
		}
		for (FieldInfo fieldInfo : fieldInfos)
			fieldInfo.trim();

		return fieldInfos;
	}

	private List<FieldInfo> processSasFile(SasFileReader sasFileReader) throws IOException, InterruptedException {
		List<FieldInfo> fieldInfos = new ArrayList<>();

		SasFileProperties sasFileProperties = sasFileReader.getSasFileProperties();
		for (Column column : sasFileReader.getColumns()) {
			FieldInfo fieldInfo = new FieldInfo(column.getName());
			fieldInfo.label = column.getLabel();
			fieldInfo.rowCount = sasFileProperties.getRowCount();
			if (!scanValues) {
				// Either NUMBER or STRING; scanning values produces a more granular type and is preferred
				fieldInfo.type = column.getType().getName().replace("java.lang.", "");
				fieldInfo.maxLength = column.getLength();
			}
			fieldInfos.add(fieldInfo);
			interrupter.checkWasInterrupted();
		}

		if (!scanValues) {
			return fieldInfos;
		}

		for (int lineNr = 0; lineNr < sasFileProperties.getRowCount(); lineNr++) {
			Object[] row = sasFileReader.readNext();

			if (row.length != fieldInfos.size()) {
				logger.warning("WARNING: row " + lineNr + " not scanned due to field count mismatch.");
				continue;
			}

			for (int i = 0; i < row.length; i++) {
				fieldInfos.get(i).processValue(row[i] == null ? "" : row[i].toString());
			}

			if (sampleSize != -1 && lineNr >= sampleSize)
				break;
		}

		for (FieldInfo fieldInfo : fieldInfos) {
			fieldInfo.trim();
		}

		return fieldInfos;
	}

	private class FieldInfo {
		public String type;
		public String name;
		public String label;
		public CountingSet<String> valueCounts = new CountingSet<>();
		public long sumLength = 0;
		public int maxLength = 0;
		public long nProcessed = 0;
		public long emptyCount = 0;
		public long uniqueCount = 0;
		public long rowCount = -1;
		public boolean isInteger = true;
		public boolean isReal = true;
		public boolean isDate = true;
		public boolean isFreeText = false;
		public boolean tooManyValues = false;
		public UniformSamplingReservoir samplingReservoir;
		public Object average;
		public Object stdev;
		public Object minimum;
		public Object maximum;
		public Object q1;
		public Object q2;
		public Object q3;

		public FieldInfo(String name) {
			this.name = name;
			if (calculateNumericStats) {
				this.samplingReservoir = new UniformSamplingReservoir(numStatsSamplerSize);
			}
		}

		public void trim() {
			// Only keep values that are used in scan report
			if (valueCounts.size() > maxValues) {
				valueCounts.keepTopN(maxValues);
			}

			// Calculate numeric stats and dereference sampling reservoir to save memory.
			if (calculateNumericStats) {
				average = getAverage();
				stdev = getStandardDeviation();
				minimum = getMinimum();
				maximum = getMaximum();
				q1 = getQ1();
				q2 = getQ2();
				q3 = getQ3();
			}
			samplingReservoir = null;
		}

		public boolean hasValuesTrimmed() {
			return tooManyValues;
		}

		public Double getFractionEmpty() {
			if (nProcessed == 0)
				return 1d;
			else
				return emptyCount / (double) nProcessed;
		}

		public String getTypeDescription() {
			if (type != null)
				return type;
			else if (!scanValues) // If not type assigned and not values scanned, do not derive
				return "";
			else if (nProcessed == emptyCount)
				return DataType.EMPTY.name();
			else if (isFreeText)
				return DataType.TEXT.name();
			else if (isDate)
				return DataType.DATE.name();
			else if (isInteger)
				return DataType.INT.name();
			else if (isReal)
				return DataType.REAL.name();
			else
				return DataType.VARCHAR.name();
		}

		public Double getFractionUnique() {
			if (nProcessed == 0 || uniqueCount == 1) {
				return 0d;
			} else {
				return uniqueCount / (double) nProcessed;
			}

		}

		public void processValue(String value) {
			nProcessed++;
			sumLength += value.length();
			if (value.length() > maxLength)
				maxLength = value.length();

			String trimValue = value.trim();
			if (trimValue.length() == 0)
				emptyCount++;

			if (!isFreeText) {
				boolean newlyAdded = valueCounts.add(value);
				if (newlyAdded) uniqueCount++;

				if (trimValue.length() != 0) {
					evaluateDataType(trimValue);
				}

				if (nProcessed == N_FOR_FREE_TEXT_CHECK && !isInteger && !isReal && !isDate) {
					doFreeTextCheck();
				}
			} else {
				valueCounts.addAll(StringUtilities.mapToWords(trimValue.toLowerCase()));
			}

			// if over this large constant number, then trimmed back to size used in report (maxValues).
			if (!tooManyValues && valueCounts.size() > MAX_VALUES_IN_MEMORY) {
				tooManyValues = true;
				this.trim();
			}

			if (calculateNumericStats && !trimValue.isEmpty()) {
				if (isInteger || isReal) {
					samplingReservoir.add(Double.parseDouble(trimValue));
				} else if (isDate) {
					samplingReservoir.add(DateUtilities.parseDate(trimValue));
				}
			}

		}

		/**
		 * Return list contains all values with frequency greater than minCellCount variable
		 * If there are values with frequency less than or equal to minCellCount added 'List truncated...' in result list
		 * Result list size equal maxValues variable, all other values are deleted
		 */
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

		private void evaluateDataType(String value) {
			if (isReal && !StringUtilities.isNumber(value))
				isReal = false;
			if (isInteger && !StringUtilities.isLong(value))
				isInteger = false;
			if (isDate && !StringUtilities.isDate(value))
				isDate = false;
		}

		private void doFreeTextCheck() {
			double averageLength = sumLength / (double) (nProcessed - emptyCount);
			if (averageLength >= MIN_AVERAGE_LENGTH_FOR_FREE_TEXT) {
				isFreeText = true;
				// Reset value count to word count
				CountingSet<String> wordCounts = new CountingSet<>();
				for (Map.Entry<String, Count> entry : valueCounts.key2count.entrySet())
					for (String word : StringUtilities.mapToWords(entry.getKey().toLowerCase()))
						wordCounts.add(word, entry.getValue().count);
				valueCounts = wordCounts;
			}
		}

		private Object formatNumericValue(double value) {
			return formatNumericValue(value, false);
		}

		private Object formatNumericValue(double value, boolean dateAsDays) {
			if (nProcessed == 0) {
				return Double.NaN;
			} else if (getTypeDescription().equals(DataType.EMPTY.name())) {
				return Double.NaN;
			} else if (isInteger || isReal) {
				return value;
			} else if (isDate && dateAsDays) {
				return value;
			} else if (isDate) {
				return LocalDate.ofEpochDay((long) value).toString();
			} else {
				return Double.NaN;
			}
		}

		private Object getMinimum() {
			double min = samplingReservoir.getPopulationMinimum();
			return formatNumericValue(min);
		}

		private Object getMaximum() {
			double max = samplingReservoir.getPopulationMaximum();
			return formatNumericValue(max);
		}

		private Object getAverage() {
			double average = samplingReservoir.getPopulationMean();
			return formatNumericValue(average);
		}

		private Object getStandardDeviation() {
			double stddev = samplingReservoir.getSampleStandardDeviation();
			return formatNumericValue(stddev, true);
		}

		private Object getQ1() {
			double q1 = samplingReservoir.getSampleQuartiles().get(0);
			return formatNumericValue(q1);
		}

		private Object getQ2() {
			double q2 = samplingReservoir.getSampleQuartiles().get(1);
			return formatNumericValue(q2);
		}

		private Object getQ3() {
			double q3 = samplingReservoir.getSampleQuartiles().get(2);
			return formatNumericValue(q3);
		}

	}

	private Row addRow(Sheet sheet, Object... values) {
		Row row = sheet.createRow(sheet.getPhysicalNumberOfRows());
		for (Object value : values) {
			Cell cell = row.createCell(row.getPhysicalNumberOfCells());

			if (value instanceof Integer || value instanceof Long || value instanceof Double) {
				double numVal = Double.parseDouble(value.toString());
				if (Double.isNaN(numVal) || Double.isInfinite(numVal)) {
					cell.setCellValue("");
				} else {
					cell.setCellValue(numVal);
				}
			} else if (value != null) {
				cell.setCellValue(value.toString());
			} else {
				cell.setCellValue("");
			}
		}
		return row;
	}

	private void setColumnStyles(Row row, CellStyle style, int... colNums) {
		for(int j : colNums) {
			Cell cell = row.getCell(j);
			if (cell != null) {
				cell.setCellStyle(style);
			}
		}
	}
}
