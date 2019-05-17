package org.ohdsi.rabbitInAHat.dataModel;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JOptionPane;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class StemTableAdd {

	public static void addStemTable(ETL etl) {
		Database sourceDatabase = etl.getSourceDatabase();
		Database targetDatabase = etl.getTargetDatabase();
		InputStream tableStream;
		InputStream mappingStream;
		if (targetDatabase.getDbName().toLowerCase().equals("cdmv5.0.1")) {
			tableStream = StemTableAdd.class.getResourceAsStream("StemTableV5.0.1.csv");
			mappingStream = StemTableAdd.class.getResourceAsStream("StemTableDefaultMappingV5.0.1.csv");
		} else if (targetDatabase.getDbName().toLowerCase().equals("cdmv5.1.0")) {
			tableStream = StemTableAdd.class.getResourceAsStream("StemTableV5.1.0.csv");
			mappingStream = StemTableAdd.class.getResourceAsStream("StemTableDefaultMappingV5.1.0.csv");
		} else if (targetDatabase.getDbName().toLowerCase().equals("cdmv5.2.0")) {
			tableStream = StemTableAdd.class.getResourceAsStream("StemTableV5.2.0.csv");
			mappingStream = StemTableAdd.class.getResourceAsStream("StemTableDefaultMappingV5.2.0.csv");
		} else if (targetDatabase.getDbName().toLowerCase().equals("cdmv5.3.0")) {
			tableStream = StemTableAdd.class.getResourceAsStream("StemTableV5.3.0.csv");
			mappingStream = StemTableAdd.class.getResourceAsStream("StemTableDefaultMappingV5.3.0.csv");
		} else if (targetDatabase.getDbName().toLowerCase().equals("cdmv5.3.1")) {
			tableStream = StemTableAdd.class.getResourceAsStream("StemTableV5.3.1.csv");
			mappingStream = StemTableAdd.class.getResourceAsStream("StemTableDefaultMappingV5.3.1.csv");
		} else if (targetDatabase.getDbName().toLowerCase().equals("cdmv6.0")) {
			tableStream = StemTableAdd.class.getResourceAsStream("StemTableV6.0.csv");
			mappingStream = StemTableAdd.class.getResourceAsStream("StemTableDefaultMappingV6.0.csv");
		} else {
			JOptionPane.showMessageDialog(null, "No stem table definition available for " + targetDatabase.getDbName(), "Error", JOptionPane.ERROR_MESSAGE);
			return;
		}

		try {
			Table sourceStemTable = new Table();
			sourceStemTable.setStem(true);
			for (CSVRecord row : CSVFormat.RFC4180.withHeader().parse(new InputStreamReader(tableStream))) {
				if (sourceStemTable.getName() == null)
					sourceStemTable.setName(row.get("TABLE_NAME").toLowerCase());
				Field field = new Field(row.get("COLUMN_NAME").toLowerCase(), sourceStemTable);
				field.setNullable(row.get("IS_NULLABLE").equals("YES"));
				field.setType(row.get("DATA_TYPE"));
				field.setDescription(row.get("DESCRIPTION"));
				field.setStem(true);
				sourceStemTable.getFields().add(field);
			}
			sourceStemTable.setDb(sourceDatabase);
			sourceDatabase.getTables().add(sourceStemTable);
			Table targetStemTable = new Table(sourceStemTable);
			targetStemTable.setDb(targetDatabase);
			targetDatabase.getTables().add(targetStemTable);

			Mapping<Table> mapping = etl.getTableToTableMapping();
			Map<String, Table> nameToTable = new HashMap<String, Table>();
			for (CSVRecord row : CSVFormat.RFC4180.withHeader().parse(new InputStreamReader(mappingStream))) {
				String targetTableName = row.get("TARGET_TABLE");
				Table targetTable = nameToTable.get(targetTableName);
				if (targetTable == null) {
					targetTable = targetDatabase.getTableByName(targetTableName);
					mapping.addSourceToTargetMap(sourceStemTable, targetTable);
					nameToTable.put(targetTableName, targetTable);
				}
				Mapping<Field> fieldToFieldMapping = etl.getFieldToFieldMapping(sourceStemTable, targetTable);
				Field sourceField = sourceStemTable.getFieldByName(row.get("SOURCE_FIELD"));
				Field targetField = targetTable.getFieldByName(row.get("TARGET_FIELD"));
				fieldToFieldMapping.addSourceToTargetMap(sourceField, targetField);
			}

		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}

	}

}
