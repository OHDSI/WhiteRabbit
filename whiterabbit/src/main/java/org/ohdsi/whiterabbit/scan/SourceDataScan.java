/*******************************************************************************
 * Copyright 2019 Observational Health Data Sciences and Informatics
 * <p>
 * This file is part of WhiteRabbit
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.whiterabbit.scan;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import com.epam.parso.Column;
import com.epam.parso.SasFileProperties;
import com.epam.parso.SasFileReader;
import com.epam.parso.impl.SasFileReaderImpl;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.commons.io.FileUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.ohdsi.databases.configuration.DbSettings;
import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.databases.QueryResult;
import org.ohdsi.databases.*;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.*;
import org.ohdsi.utilities.collections.Pair;
import org.ohdsi.utilities.files.ReadTextFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Long.max;

public class SourceDataScan implements ScanParameters {
	static Logger logger = LoggerFactory.getLogger(SourceDataScan.class);
	public final static String SCAN_REPORT_FILE_NAME = "ScanReport.xlsx";

	public static final String POI_TMP_DIR_ENVIRONMENT_VARIABLE_NAME = "ORG_OHDSI_WHITERABBIT_POI_TMPDIR";
	public static final String POI_TMP_DIR_PROPERTY_NAME = "org.ohdsi.whiterabbit.poi.tmpdir";

	private XSSFWorkbook workbook;
	private char delimiter = ',';
	private int sampleSize;
	private boolean scanValues = false;
	private boolean calculateNumericStats = false;
	private int numStatsSamplerSize;
	private int minCellCount;
	private int maxValues;
	private DbSettings.SourceType sourceType;
	private DbType dbType;
	private Map<Table, List<FieldInfo>> tableToFieldInfos;
	private Map<String, String> indexedTableNameLookup;

	private LocalDateTime startTimeStamp;

	static final String poiTmpPath;

	static {
		try {
			poiTmpPath = setUniqueTempDirStrategyForApachePoi();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void setSampleSize(int sampleSize) {
		// -1 if sample size is not restricted
		this.sampleSize = sampleSize;
	}

	public boolean doCalculateNumericStats() {
		return calculateNumericStats;
	}

	public int getMaxValues() {
		return maxValues;
	}

	public boolean doScanValues() {
		return scanValues;
	}

	public int getNumStatsSamplerSize() {
		return numStatsSamplerSize;
	}

	public void setScanValues(boolean scanValues) {
		this.scanValues = scanValues;
	}

	public void setMinCellCount(int minCellCount) {
		this.minCellCount = minCellCount;
	}

	public int getMinCellCount() {
		return minCellCount;
	}

	public int getSampleSize() {
		return sampleSize;
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

	public void process(DbSettings dbSettings, String outputFileName) throws IOException {
		startTimeStamp = LocalDateTime.now();
		sourceType = dbSettings.sourceType;
		dbType = dbSettings.dbType;

		tableToFieldInfos = new HashMap<>();
		StringUtilities.outputWithTime("Started new scan of " + dbSettings.tables.size() + " tables...");
		if (sourceType == DbSettings.SourceType.CSV_FILES) {
			if (!scanValues)
				this.minCellCount = Math.max(minCellCount, MIN_CELL_COUNT_FOR_CSV);
			processCsvFiles(dbSettings);
		} else if (sourceType == DbSettings.SourceType.SAS_FILES) {
			processSasFiles(dbSettings);
		} else {
			processDatabase(dbSettings);
		}

		generateReport(outputFileName);
	}

	/*
	 * Implements a strategy for the tmp dir to ise for files for apache poi
	 * Attempts to solve an issue where some users report not having write access to the poi tmp dir
	 * (see https://github.com/OHDSI/WhiteRabbit/issues/293). Vry likely this is caused by the poi tmp dir
	 * being created on a multi-user system by a user with a too restrictive file mask.
	 */
	public static String setUniqueTempDirStrategyForApachePoi() throws IOException {
		Path myTmpDir = getDefaultPoiTmpPath(FileUtils.getTempDirectory().toPath());
		String userConfiguredPoiTmpDir = getUserConfiguredPoiTmpDir();
		if (!StringUtils.isEmpty(userConfiguredPoiTmpDir)) {
			myTmpDir = setupTmpDir(Paths.get(userConfiguredPoiTmpDir));
		} else {
			if (isNotWritable(myTmpDir)) {
				// avoid the poi files directory entirely by creating a separate directory in the standard tmp dir
				myTmpDir = setupTmpDir(FileUtils.getTempDirectory().toPath());
			}
		}

		String tmpDir = myTmpDir.toFile().getAbsolutePath();
		checkWritableTmpDir(tmpDir);
		return tmpDir;
	}

	public static Path getDefaultPoiTmpPath(Path tmpRoot) {
		// TODO: if/when updating poi to 5.x or higher, use DefaultTempFileCreationStrategy.POIFILES instead of a string literal
		final String poiFilesDir = "poifiles"; // copied from poi implementation 4.x
		return tmpRoot.resolve(poiFilesDir);
	}

	private static Path setupTmpDir(Path tmpDir) {
		checkWritableTmpDir(tmpDir.toFile().getAbsolutePath());
		Path myTmpDir = Paths.get(tmpDir.toFile().getAbsolutePath(), UUID.randomUUID().toString());
		try {
			Files.createDirectory(myTmpDir);
			org.apache.poi.util.TempFile.setTempFileCreationStrategy(new org.apache.poi.util.DefaultTempFileCreationStrategy(myTmpDir.toFile()));
		} catch (IOException ioException) {
			throw new RuntimeException(String.format("Exception while creating directory %s", myTmpDir), ioException);
		}
		return myTmpDir;
	}

	private static void checkWritableTmpDir(String dir) {
		if (isNotWritable(Paths.get(dir))) {
			String message = String.format("Directory %s is not writable! (used for tmp files for Apache POI)", dir);
			logger.warn(message);
			throw new RuntimeException(message);
		}
	}

	private static String getUserConfiguredPoiTmpDir() {
		// search for a user configured dir for poi tmp files. Env.var. overrules Java property.
		String userConfiguredDir = System.getenv(POI_TMP_DIR_ENVIRONMENT_VARIABLE_NAME);
		if (StringUtils.isEmpty(userConfiguredDir)) {
			userConfiguredDir = System.getProperty(POI_TMP_DIR_PROPERTY_NAME);
		}
		return userConfiguredDir;
	}

	public static boolean isNotWritable(Path path) {
		final Path testFile = path.resolve("test.txt");
		if (Files.exists(path) && Files.isDirectory(path)) {
			try {
				Files.createFile(testFile);
				Files.delete(testFile);
			} catch (IOException e) {
				return true;
			}
			return false;
		}
		return true;
	}

	private void processDatabase(DbSettings dbSettings) {
		// GBQ requires database. Put database value into domain var
		if (dbSettings.dbType == DbType.BIGQUERY) {
			dbSettings.domain = dbSettings.database;
		}
		try (RichConnection connection = new RichConnection(dbSettings)) {
			connection.setVerbose(false);
			connection.use(dbSettings.database);

			tableToFieldInfos = dbSettings.tables.stream()
					.collect(Collectors.toMap(
							Table::new,
							table -> processDatabaseTable(table, connection, dbSettings.database)
					));
		}
	}

	private void processCsvFiles(DbSettings dbSettings) {
		delimiter = dbSettings.delimiter;
		for (String fileName : dbSettings.tables) {
			Table table = new Table();
			table.setName(new File(fileName).getName());
			List<FieldInfo> fieldInfos = processCsvFile(fileName);
			tableToFieldInfos.put(table, fieldInfos);
		}
	}

	private void processSasFiles(DbSettings dbSettings) {
		for (String fileName : dbSettings.tables) {
			try(FileInputStream inputStream = new FileInputStream(new File(fileName))) {
				SasFileReader sasFileReader = new SasFileReaderImpl(inputStream);
				SasFileProperties sasFileProperties = sasFileReader.getSasFileProperties();

				Table table = new Table(new File(fileName).getName());
				table.setName(new File(fileName).getName());
				table.setComment(sasFileProperties.getName());

				StringUtilities.outputWithTime("Scanning table " + fileName);
				List<FieldInfo> fieldInfos = processSasFile(sasFileReader);
				tableToFieldInfos.put(table, fieldInfos);

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void generateReport(String filename) throws IOException {
		StringUtilities.outputWithTime("Generating scan report");
		removeEmptyTables();

		workbook = new XSSFWorkbook();

		int i = 0;
		indexedTableNameLookup = new HashMap<>();
		for (Table table : tableToFieldInfos.keySet()) {
			String tableNameIndexed = Table.indexTableNameForSheet(table.getName(), i);
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
			StringUtilities.outputWithTime("Scan report generated: " + filename);
		} catch (IOException ex) {
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
							fieldInfo.hasValuesTrimmed() ? String.format(Locale.ENGLISH, "<= %.3f", fractionUnique) : fractionUnique
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
		addRow(metaSheet, "dbType", this.dbType != null ? this.dbType.name() : "");
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
				.removeIf(stringListEntry -> stringListEntry.getValue().isEmpty());
	}

	private List<FieldInfo> processDatabaseTable(String table, RichConnection connection, String database) {
		StringUtilities.outputWithTime("Scanning table " + table);

		long rowCount;
		if (connection.getConnection().hasStorageHandler()) {
			rowCount = connection.getConnection().getStorageHandler().getTableSize(table);
		} else {
			rowCount = connection.getTableSize(table);
		}
		List<FieldInfo> fieldInfos = connection.fetchTableStructure(connection, database, table, this);
		if (scanValues) {
			int actualCount = 0;
			QueryResult queryResult = null;
			try {
				queryResult = connection.fetchRowsFromTable(table, rowCount, this);
				for (org.ohdsi.utilities.files.Row row : queryResult) {
					for (FieldInfo fieldInfo : fieldInfos) {
						fieldInfo.processValue(row.get(fieldInfo.name));
					}
					actualCount++;
					if (sampleSize != -1 && actualCount >= sampleSize) {
						logger.info("Stopped after {} rows", actualCount);
						break;
					}
				}
				for (FieldInfo fieldInfo : fieldInfos)
					fieldInfo.trim();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} finally {
				if (queryResult != null) {
					queryResult.close();
				}
			}
		}

		return fieldInfos;
	}


	private List<FieldInfo> processCsvFile(String filename) {
		StringUtilities.outputWithTime("Scanning table " + filename);
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
					fieldInfos.add(new FieldInfo(this, cell));
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

	private List<FieldInfo> processSasFile(SasFileReader sasFileReader) throws IOException {
		List<FieldInfo> fieldInfos = new ArrayList<>();

		SasFileProperties sasFileProperties = sasFileReader.getSasFileProperties();
		for (Column column : sasFileReader.getColumns()) {
			FieldInfo fieldInfo = new FieldInfo(this, column.getName());
			fieldInfo.label = column.getLabel();
			fieldInfo.rowCount = sasFileProperties.getRowCount();
			if (!scanValues) {
				// Either NUMBER or STRING; scanning values produces a more granular type and is preferred
				fieldInfo.type = column.getType().getName().replace("java.lang.", "");
				fieldInfo.maxLength = column.getLength();
			}
			fieldInfos.add(fieldInfo);
		}

		if (!scanValues) {
			return fieldInfos;
		}

		for (int lineNr = 0; lineNr < sasFileProperties.getRowCount(); lineNr++) {
			Object[] row = sasFileReader.readNext();

			if (row.length != fieldInfos.size()) {
				StringUtilities.outputWithTime("WARNING: row " + lineNr + " not scanned due to field count mismatch.");
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
