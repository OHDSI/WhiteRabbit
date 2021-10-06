package org.ohdsi.rabbitInAHat.dataModel;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JOptionPane;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.ohdsi.utilities.collections.Pair;

public class StemTableFactory {

	private static final String STEM_TABLE_NAME = "stem_table";

	public static void addStemTable(ETL etl) {
		Database sourceDatabase = etl.getSourceDatabase();
		Database targetDatabase = etl.getTargetDatabase();
		InputStream tableStream;
		InputStream mappingStream;
		if (targetDatabase.getDbName().equalsIgnoreCase("cdmv5.0")) {
			tableStream = StemTableFactory.class.getResourceAsStream("StemTableV5.0.csv");
			mappingStream = StemTableFactory.class.getResourceAsStream("StemTableDefaultMappingV5.0.csv");
		} else if (targetDatabase.getDbName().equalsIgnoreCase("cdmv5.1")) {
			tableStream = StemTableFactory.class.getResourceAsStream("StemTableV5.1.csv");
			mappingStream = StemTableFactory.class.getResourceAsStream("StemTableDefaultMappingV5.1.csv");
		} else if (targetDatabase.getDbName().equalsIgnoreCase("cdmv5.2")) {
			tableStream = StemTableFactory.class.getResourceAsStream("StemTableV5.2.csv");
			mappingStream = StemTableFactory.class.getResourceAsStream("StemTableDefaultMappingV5.2.csv");
		} else if (targetDatabase.getDbName().equalsIgnoreCase("cdmv5.3")) {
			tableStream = StemTableFactory.class.getResourceAsStream("StemTableV5.3.csv");
			mappingStream = StemTableFactory.class.getResourceAsStream("StemTableDefaultMappingV5.3.csv");
		} else if (targetDatabase.getDbName().equalsIgnoreCase("cdmv5.4")) {
			tableStream = StemTableFactory.class.getResourceAsStream("StemTableV5.4.csv");
			mappingStream = StemTableFactory.class.getResourceAsStream("StemTableDefaultMappingV5.4.csv");
		} else if (targetDatabase.getDbName().equalsIgnoreCase("cdmv6.0")) {
			tableStream = StemTableFactory.class.getResourceAsStream("StemTableV6.0.csv");
			mappingStream = StemTableFactory.class.getResourceAsStream("StemTableDefaultMappingV6.0.csv");
		} else {
			JOptionPane.showMessageDialog(null, "No stem table definition available for " + targetDatabase.getDbName(), "Error", JOptionPane.ERROR_MESSAGE);
			return;
		}

		try {
			Table sourceStemTable = new Table();
			sourceStemTable.setStem(true);
			sourceStemTable.setName(STEM_TABLE_NAME);
			CSVFormat csvFormat = CSVFormat.Builder.create(CSVFormat.RFC4180).setHeader().build();
			for (CSVRecord row : csvFormat.parse(new InputStreamReader(tableStream))) {
				Field field = new Field(row.get("COLUMN_NAME").toLowerCase(), sourceStemTable);
				field.setNullable(row.get("IS_NULLABLE").equals("YES"));
				field.setType(row.get("DATA_TYPE"));
				field.setDescription(row.get("DESCRIPTION"));
				field.setStem(true);
				sourceStemTable.getFields().add(field);
			}
			sourceDatabase.getTables().add(sourceStemTable);
			Table targetStemTable = new Table(sourceStemTable);
			targetStemTable.setDb(targetDatabase);
			targetDatabase.getTables().add(targetStemTable);

			Mapping<Table> mapping = etl.getTableToTableMapping();
			Map<String, Table> nameToTable = new HashMap<>();
			for (CSVRecord row : csvFormat.parse(new InputStreamReader(mappingStream))) {
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

	public static void removeStemTable(ETL etl) {
		// Find stem source and target tables
		Mapping<Table> mapping = etl.getTableToTableMapping();
		List<Pair<Table, Table>> tablesMappingsToRemove = new ArrayList<>();
		for (ItemToItemMap sourceToTargetMap : mapping.getSourceToTargetMaps()) {
			Table sourceTable = (Table) sourceToTargetMap.getSourceItem();
			Table targetTable = (Table) sourceToTargetMap.getTargetItem();
			if (sourceTable.isStem() || targetTable.isStem()) {
				tablesMappingsToRemove.add(new Pair<>(sourceTable, targetTable));
			}
		}

		// Remove stem table to table and field to field mappings
		for (Pair<Table, Table> tableTablePair : tablesMappingsToRemove) {
			etl.getFieldToFieldMapping(tableTablePair.getItem1(), tableTablePair.getItem2()).removeAllSourceToTargetMaps();
			mapping.removeSourceToTargetMap(tableTablePair.getItem1(), tableTablePair.getItem2());
		}

		// Remove stem source table
		Database sourceDatabase = etl.getSourceDatabase();
		List<Table> newSourceTables = new ArrayList<>();
		for (Table table : sourceDatabase.getTables()) {
			if (!table.isStem()) {
				newSourceTables.add(table);
			}
		}
		sourceDatabase.setTables(newSourceTables);

		// Remove stem target table
		Database targetDatabase = etl.getTargetDatabase();
		List<Table> newTargetTables = new ArrayList<>();
		for (Table table : targetDatabase.getTables()) {
			if (!table.isStem()) {
				newTargetTables.add(table);
			}
		}
		targetDatabase.setTables(newTargetTables);
	}

}
