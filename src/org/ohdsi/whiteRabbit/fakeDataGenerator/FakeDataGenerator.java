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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.OneToManySet;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteCSVFileWithHeader;
import org.ohdsi.whiteRabbit.DbSettings;

public class FakeDataGenerator {

	private RichConnection					connection;
	// private DbType dbType;
	private int								targetType;
	private OneToManySet<String, String>	primaryKeyToValues;
	private int								maxRowsPerTable	= 1000;

	private static int						REGULAR			= 0;
	private static int						RANDOM			= 1;
	private static int						PRIMARY_KEY		= 2;

	public static void main(String[] args) {
		FakeDataGenerator fakeDataGenerator = new FakeDataGenerator();

		DbSettings dbSettings = new DbSettings();
		dbSettings.dataType = DbSettings.DATABASE;
		dbSettings.dbType = DbType.POSTGRESQL;
		dbSettings.server = "127.0.0.1/ohdsi";
		dbSettings.database = "ars";
		dbSettings.user = "postgres";
		dbSettings.password = "F1r3starter";

		fakeDataGenerator.generateData(dbSettings, 100000, "c:/temp/ScanReport.xlsx", "c:/temp");
		// fakeDataGenerator.generateData(dbSettings, 1000, "C:/home/Research/EMIF WP12/ARS CDM loading/ScanReport.xlsx", "c:/temp");
	}

	public void generateData(DbSettings dbSettings, int maxRowsPerTable, String filename, String folder) {
		this.maxRowsPerTable = maxRowsPerTable;
		// this.dbType = dbSettings.dbType;
		this.targetType = dbSettings.dataType;

		StringUtilities.outputWithTime("Starting creation of fake data");
		System.out.println("Loading scan report from " + filename);
		Database database = Database.generateModelFromScanReport(filename);
		findValuesForPrimaryKeys(database);

		if (targetType == DbSettings.DATABASE) {
			connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
			connection.use(dbSettings.database);
			for (Table table : database.getTables()) {
				if (table.getName().toLowerCase().endsWith(".csv"))
					table.setName(table.getName().substring(0, table.getName().length() - 4));
				System.out.println("Generating table " + table.getName());
				createTable(table);
				connection.insertIntoTable(generateRows(table).iterator(), table.getName(), false);
			}
			connection.close();
		} else {
			for (Table table : database.getTables()) {
				String name = folder + "/" + table.getName();
				if (!name.toLowerCase().endsWith(".csv"))
					name = name + ".csv";
				System.out.println("Generating table " + name);
				WriteCSVFileWithHeader out = new WriteCSVFileWithHeader(name, dbSettings.csvFormat);
				for (Row row : generateRows(table))
					out.write(row);
				out.close();
			}
		}
		StringUtilities.outputWithTime("Done");
	}

	private void findValuesForPrimaryKeys(Database database) {
		Set<String> primaryKeys = new HashSet<String>();
		for (Table table : database.getTables()) {
			for (Field field : table.getFields()) {
				if (field.getValueCounts()[0][0].equals("List truncated...")) {
					primaryKeys.add(field.getName());
				}
			}
		}

		primaryKeyToValues = new OneToManySet<String, String>();
		for (Table table : database.getTables()) {
			for (Field field : table.getFields()) {
				if (primaryKeys.contains(field.getName()) && !field.getValueCounts()[0][0].equals("List truncated...")) {
					for (int i = 0; i < field.getValueCounts().length; i++)
						if (!field.getValueCounts()[i][0].equals("") && !field.getValueCounts()[i][0].equals("List truncated..."))
							primaryKeyToValues.put(field.getName(), field.getValueCounts()[i][0]);
				}
			}
		}
	}

	private List<Row> generateRows(Table table) {
		String[] fieldNames = new String[table.getFields().size()];
		ValueGenerator[] valueGenerators = new ValueGenerator[table.getFields().size()];
		int size = maxRowsPerTable;
		for (int i = 0; i < table.getFields().size(); i++) {
			Field field = table.getFields().get(i);
			fieldNames[i] = field.getName();
			ValueGenerator valueGenerator = new ValueGenerator(field);
			valueGenerators[i] = valueGenerator;
			if (valueGenerator.generatorType == PRIMARY_KEY && valueGenerator.values.length < size)
				size = valueGenerator.values.length;
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

	// private String correctType(Field field) {
	// String type = field.getType().toUpperCase();
	// if (field.getMaxLength() == 0)
	// field.setMaxLength(256);
	// if (dbType == DbType.MYSQL) {
	// if (isVarChar(type))
	// return "VARCHAR(" + field.getMaxLength() + ")";
	// else if (isInt(type))
	// return "BIGINT";
	// else if (isNumber(type))
	// return "DOUBLE";
	// else if (isText(type))
	// return "TEXT";
	// else if (type.equals("EMPTY"))
	// return "VARCHAR(255)";
	// else
	// return type;
	// } else if (dbType == DbType.POSTGRESQL) {
	// if (isVarChar(type))
	// return "VARCHAR(" + field.getMaxLength() + ")";
	// else if (isInt(type))
	// return "BIGINT";
	// else if (isNumber(type))
	// return "DOUBLE";
	// else if (isText(type))
	// return "TEXT";
	// else if (type.equals("EMPTY"))
	// return "VARCHAR(255)";
	// else
	// return type;
	// }
	// return null;
	// }

	private boolean isVarChar(String type) {
		type = type.toUpperCase();
		return (type.equals("VARCHAR") || type.equals("VARCHAR2") || type.equals("CHARACTER VARYING"));
	}

	private boolean isInt(String type) {
		type = type.toUpperCase();
		return (type.equals("INT") || type.equals("INTEGER") || type.equals("BIGINT"));
	}

	// private boolean isNumber(String type) {
	// type = type.toUpperCase();
	// return (type.equals("REAL") || type.equals("DOUBLE") || type.equals("NUMBER") || type.equals("FLOAT") || type.equals("DOUBLE PRECISION"));
	// }
	//
	// private boolean isText(String type) {
	// type = type.toUpperCase();
	// return (type.equals("TEXT") || type.equals("CLOB"));
	// }

	private class ValueGenerator {

		private String[]	values;
		private int[]		cumulativeFrequency;
		private int			totalFrequency;
		private String		type;
		private int			length;
		private int			cursor;
		private int			generatorType	= REGULAR;
		private Random		random			= new Random();

		public ValueGenerator(Field field) {
			String[][] valueCounts = field.getValueCounts();
			type = field.getType();
			if (valueCounts[0][0].equals("List truncated...")) {
				Set<String> values = primaryKeyToValues.get(field.getName());
				if (values.size() != 0) {
					this.values = convertToArray(values);
					cursor = 0;
					generatorType = PRIMARY_KEY;
				} else {
					length = field.getMaxLength();
					generatorType = RANDOM;
				}
			} else {
				int length = valueCounts.length;
				if (valueCounts[length - 1][1].equals("")) // Last value could be "List truncated..."
					length--;

				values = new String[length];
				cumulativeFrequency = new int[length];
				totalFrequency = 0;
				for (int i = 0; i < length; i++) {
					int frequency = (int) (Double.parseDouble(valueCounts[i][1]));
					totalFrequency += frequency;

					values[i] = valueCounts[i][0];
					cumulativeFrequency[i] = totalFrequency;
				}
				generatorType = REGULAR;
			}
		}

		private String[] convertToArray(Set<String> set) {
			String[] array = new String[set.size()];
			int i = 0;
			for (String item : set)
				array[i++] = item;
			return array;
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
			} else if (generatorType == PRIMARY_KEY) { // Pick the next value:
				String value = values[cursor];
				cursor++;
				if (cursor >= values.length)
					cursor = 0;
				return value;
			} else { // Sample from values:
				int index = random.nextInt(totalFrequency);
				int i = 0;
				while (i < values.length - 1 && cumulativeFrequency[i] < index)
					i++;
				if (!type.equals("VarChar") && values[i].trim().length() == 0)
					return "";
				return values[i];
			}
		}
	}

}
