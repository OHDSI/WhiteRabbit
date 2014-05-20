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
package org.ohdsi.whiteRabbit.fakeDataGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.whiteRabbit.DbSettings;
import org.ohdsi.whiteRabbit.interactiveMapping.dataModel.Database;
import org.ohdsi.whiteRabbit.interactiveMapping.dataModel.Field;
import org.ohdsi.whiteRabbit.interactiveMapping.dataModel.Table;
import org.ohdsi.whiteRabbit.utilities.ETLUtils;

public class FakeDataGenerator {

	private RichConnection	connection;
	private DbType			dbType;

	public static int		ROWS_PER_TABLE	= 1000;

	public static void main(String[] args) {
		FakeDataGenerator fakeDataGenerator = new FakeDataGenerator();
		
		DbSettings dbSettings = new DbSettings();
//		dbSettings.dataType = DbSettings.DATABASE;
//		dbSettings.dbType = DbType.MYSQL;
//		dbSettings.server = "127.0.0.1";
//		dbSettings.database = "fake_data";
//		dbSettings.user = "root";
//		dbSettings.password = "F1r3starter";
//		fakeDataGenerator.generateData(dbSettings, "S:/Data/THIN/ScanReport.xlsx",true);
		
		dbSettings.dataType = DbSettings.DATABASE;
		dbSettings.dbType = DbType.MSSQL;
		dbSettings.server = "RNDUSRDHIT03.jnj.com";
		dbSettings.database = "CDM_THIN";
		fakeDataGenerator.generateData(dbSettings, "S:/Data/THIN/ScanReport.xlsx",false);
	}

	private void generateData(DbSettings dbSettings, String filename, boolean createDatabase) {
		System.out.println("Loading scan report from " + filename);
		Database database = Database.generateModelFromScanReport(filename);

		dbType = dbSettings.dbType;
		if (createDatabase && ETLUtils.databaseAlreadyExists(dbSettings))
			return;
		connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		if (createDatabase)
			connection.execute("CREATE DATABASE IF NOT EXISTS `" + dbSettings.database + "`");
		connection.use(dbSettings.database);

		for (Table table : database.getTables()) {
			System.out.println("Generating table " + table.getName());
			createTable(table);
			populateTable(table);
		}

		connection.close();
	}

	private void populateTable(Table table) {
		String[] fieldNames = new String[table.getFields().size()];
		ValueGenerator[] valueGenerators = new ValueGenerator[table.getFields().size()];
		for (int i = 0; i < table.getFields().size(); i++) {
			Field field = table.getFields().get(i);
			fieldNames[i] = field.getName();
			if (fieldNames[i].equals("data1"))
				System.out.println("asdf");
			valueGenerators[i] = new ValueGenerator(field);
		}
		List<Row> rows = new ArrayList<Row>();
		for (int i = 0; i < ROWS_PER_TABLE; i++) {
			Row row = new Row();
			for (int j = 0; j < fieldNames.length; j++) {
				row.add(fieldNames[j], valueGenerators[j].generate());
			}
			rows.add(row);
		}
		connection.insertIntoTable(rows.iterator(), table.getName(), false);
	}

	private void createTable(Table table) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE " + table.getName() + " (\n");
		for (int i = 0; i < table.getFields().size(); i++) {
			Field field = table.getFields().get(i);
			sql.append("  " + field.getName() + " " + correctType(field));
			if (i < table.getFields().size() - 1)
				sql.append(",\n");
		}
		sql.append("\n);");
		connection.execute(sql.toString());
	}

	private String correctType(Field field) {
		if (field.getType().equals("VarChar"))
			return "VARCHAR(" + field.getMaxLength() + ")";
		else if (field.getType().equals("Integer"))
			return "BIGINT";
		else if (field.getType().equals("Date"))
			return "DATE";
		else if (field.getType().equals("Real"))
			return "DOUBLE";
		else if (field.getType().equals("Empty"))
			return "VARCHAR(255)";

		return null;
	}

	private class ValueGenerator {

		private String[]	values;
		private int[]		cumulativeFrequency;
		private int			totalFrequency;
		private String		type;

		private boolean		randomGenerate;

		private int			length;

		private Random		random	= new Random();

		public ValueGenerator(Field field) {
			String[][] valueCounts = field.getValueCounts();
			type = field.getType();
			if (valueCounts[0][0].equals("List truncated...")) {
				randomGenerate = true;
				length = field.getMaxLength();
			} else {
				randomGenerate = false;
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
			}
		}

		public String generate() {
			if (randomGenerate) { // Random generate a string:
				if (type.equals("VarChar")) {
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < length; i++)
						sb.append(Character.toChars(65 + random.nextInt(26)));
					return sb.toString();
				} else if (type.equals("Integer")) {
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
