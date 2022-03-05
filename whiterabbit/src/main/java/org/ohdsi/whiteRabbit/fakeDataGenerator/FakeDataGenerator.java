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
package org.ohdsi.whiteRabbit.fakeDataGenerator;

import org.ohdsi.databases.RichConnection;
import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.rabbitInAHat.dataModel.ValueCounts;
import org.ohdsi.whiteRabbit.ConsoleLogger;
import org.ohdsi.whiteRabbit.Logger;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteCSVFileWithHeader;
import org.ohdsi.whiteRabbit.Interrupter;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.ThreadInterrupter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FakeDataGenerator {

	private RichConnection connection;
	private int maxRowsPerTable = 1000;
	private boolean doUniformSampling;

	private static int REGULAR = 0;
	private static int RANDOM = 1;
	private static int PRIMARY_KEY = 2;

	private Logger logger = new ConsoleLogger();
	private Interrupter interrupter = new ThreadInterrupter();

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public void setInterrupter(Interrupter interrupter) {
		this.interrupter = interrupter;
	}

	public void generateData(DbSettings dbSettings, int maxRowsPerTable, String filename, String folder,
							 boolean doUniformSampling) throws InterruptedException {
		generateData(dbSettings, maxRowsPerTable, filename, folder, doUniformSampling,
				null, true);
	}

	/* Schema name can be null */
	public void generateData(DbSettings dbSettings, int maxRowsPerTable, String filename, String folder,
							 boolean doUniformSampling, String schemaName, boolean createTables) throws InterruptedException {
		this.maxRowsPerTable = maxRowsPerTable;
		DbSettings.SourceType targetType = dbSettings.sourceType;
		this.doUniformSampling = doUniformSampling;

		logger.info("Starting creation of fake data");

		Database database = Database.generateModelFromScanReport(filename, schemaName);
		logger.setItemsCount(database.getTables().size());

		if (targetType == DbSettings.SourceType.DATABASE) {
			connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
			connection.use(dbSettings.database);
			for (Table table : database.getTables()) {
				interrupter.checkWasInterrupted();
				if (table.getName().toLowerCase().endsWith(".csv"))
					table.setName(table.getName().substring(0, table.getName().length() - 4));
				logger.info("Generating table " + table.getName());
				if (createTables) {
					createTable(table);
				}
				try {
					connection.insertIntoTable(generateRows(table).iterator(), table.getName(), false);
					logger.incrementScannedItems();
					logger.info("Generated table " + table.getName());
				} catch (SQLException e) {
					connection.close();
					throw new RuntimeException(e);
				}
			}
			connection.close();
		} else {
			for (Table table : database.getTables()) {
				String name = folder + "/" + table.getName();
				if (!name.toLowerCase().endsWith(".csv"))
					name = name + ".csv";
				logger.info("Generating table " + name);
				WriteCSVFileWithHeader out = new WriteCSVFileWithHeader(name, dbSettings.csvFormat);
				for (Row row : generateRows(table))
					out.write(row);
				out.close();
			}
		}
		logger.info("Fake data successfully generated");
	}

	private List<Row> generateRows(Table table) {
		if (table.getRowCount() == 0 || table.getRowsCheckedCount() == 0) {
			// Empty table, return empty list (writes empty file)
			return new ArrayList<>();
		}

		String[] fieldNames = new String[table.getFields().size()];
		ValueGenerator[] valueGenerators = new ValueGenerator[table.getFields().size()];
		int size = maxRowsPerTable;
		for (int i = 0; i < table.getFields().size(); i++) {
			Field field = table.getFields().get(i);
			fieldNames[i] = field.getName();
			ValueGenerator valueGenerator = new ValueGenerator(field);
			valueGenerators[i] = valueGenerator;
//			if (valueGenerator.generatorType == PRIMARY_KEY && valueGenerator.values.length < size)
//				size = valueGenerator.values.length;
		}
		List<Row> rows = new ArrayList<Row>();
		for (int i = 0; i < size; i++) {
			Row row = new Row();
			for (int j = 0; j < fieldNames.length; j++)
				row.add(fieldNames[j], valueGenerators[j].generate());
			rows.add(row);
		}
		return rows;
	}

	private void createTable(Table table) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE " + table.getName() + " (\n");
		for (int i = 0; i < table.getFields().size(); i++) {
			Field field = table.getFields().get(i);
			sql.append("  " + field.getName() + " " + field.getType().toUpperCase());
			if (i < table.getFields().size() - 1)
				sql.append(",\n");
		}
		sql.append("\n);");
		connection.execute(sql.toString());
	}

	private boolean isVarChar(String type) {
		type = type.toUpperCase();
		return (type.equals("VARCHAR") || type.equals("VARCHAR2") || type.equals("CHARACTER VARYING"));
	}

	private boolean isInt(String type) {
		type = type.toUpperCase();
		return (type.equals("INT") || type.equals("INTEGER") || type.equals("BIGINT"));
	}

	private class ValueGenerator {

		private String[] values;
		private int[] cumulativeFrequency;
		private int totalFrequency;
		private String fieldName;
		private String type;
		private int length;
		private int pk_cursor;
		private int generatorType;
		private Random random = new Random();
		private boolean isNotUniqueWarningShown = false;

		public ValueGenerator(Field field) {
			ValueCounts valueCounts = field.getValueCounts();
			fieldName = field.getName();
			type = field.getType();
			boolean isUnique = field.getFractionUnique() != null && field.getFractionUnique() == 1;

			if (valueCounts.isEmpty()) {
				length = field.getMaxLength();
				generatorType = RANDOM;
			} else {
				int length = valueCounts.size();
				int runningTotal = 0;
				values = new String[length];
				cumulativeFrequency = new int[length];
				for (int i = 0; i < length; i++) {
					values[i] = valueCounts.get(i).getValue();
					int frequency;
					if (doUniformSampling) {
						frequency = 1;
					} else {
						frequency = valueCounts.get(i).getFrequency();
					}
					runningTotal += frequency;
					cumulativeFrequency[i] = runningTotal;
				}
				totalFrequency = runningTotal;
				generatorType = REGULAR;
			}
			if (isUnique) {
				generatorType = PRIMARY_KEY;
				pk_cursor = 0;
			}
		}

		public String generate() {
			if (generatorType == RANDOM) { // Random generate a string:
				if (isVarChar(type)) {
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < length; i++)
						sb.append(Character.toChars(65 + random.nextInt(26)));
					return sb.toString();
				} else if (isInt(type)) {
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < length; i++)
						sb.append(Character.toChars(48 + random.nextInt(10)));
					return sb.toString();
				} else if (type.equals("Date")) // todo: add code
					return "";
				else if (type.equals("Real")) // todo: add code
					return "";
				else if (type.equals("Empty"))
					return "";
				else
					return "";
			} else if (generatorType == PRIMARY_KEY) {
				// Pick the next value or use the pk_cursor
				String value;
				if (values != null) {
					if (pk_cursor >= values.length) {
						// Loop back to the first (not a primary key anymore!)
						pk_cursor = 0;
						if (!isNotUniqueWarningShown) {
							StringUtilities.outputWithTime("Used all the known " + values.length + " values for unique field '" + fieldName + "'. The values are recycled and the fake data in this column will not be unique.");
							isNotUniqueWarningShown = true;
						}
					}
					value = values[pk_cursor++];
				} else {
					value = String.valueOf(++pk_cursor);
				}
				return value;
			} else { // Sample from values:
				int index = random.nextInt(totalFrequency);
				int i = 0;
				while (i < values.length - 1 && cumulativeFrequency[i] <= index)
					i++;
				if (!type.equals("VarChar") && values[i].trim().length() == 0)
					return "";
				return values[i];
			}
		}
	}
}
