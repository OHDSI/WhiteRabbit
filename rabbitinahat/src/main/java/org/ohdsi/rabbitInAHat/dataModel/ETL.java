/*
 ******************************************************************************
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
 *****************************************************************************
 */
package org.ohdsi.rabbitInAHat.dataModel;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import com.cedarsoftware.util.io.JsonIoException;
import com.cedarsoftware.util.io.JsonReader;
import com.cedarsoftware.util.io.JsonWriter;

public class ETL implements Serializable {
	public enum FileFormat {
		Binary, Json, GzipJson
	}

	private Database								sourceDb					= new Database();
	private Database								cdmDb						= new Database();
	private final List<ItemToItemMap>						tableToTableMaps			= new ArrayList<>();
	private final Map<ItemToItemMap, List<ItemToItemMap>>	tableMapToFieldToFieldMaps	= new HashMap<>();
	private transient String						filename					= null;
	private static final long						serialVersionUID			= 8987388381751618498L;

	public ETL() {
	}

	public ETL(Database sourceDB, Database targetDb) {
		this.setSourceDatabase(sourceDB);
		this.setTargetDatabase(targetDb);
	}

	public void copyETLMappings(ETL etl) {
		Mapping<Table> oldTableMapping = etl.getTableToTableMapping();
		Mapping<Table> newTableMapping = this.getTableToTableMapping();

		for (Table sourceTable : sourceDb.getTables()) {
			for (Table targetTable : cdmDb.getTables()) {

				ItemToItemMap copyMapping = oldTableMapping.getSourceToTargetMapByName(sourceTable, targetTable);

				if (copyMapping != null) {
					copyMapping.setSourceItem(sourceTable);
					copyMapping.setTargetItem(targetTable);

					newTableMapping.addSourceToTargetMap(copyMapping);

					Mapping<Field> oldFieldMapping = etl.getFieldToFieldMapping(sourceTable, targetTable);
					Mapping<Field> newFieldMapping = this.getFieldToFieldMapping(sourceTable, targetTable);

					for (Field sourceField : sourceTable.getFields()) {
						for (Field targetField : targetTable.getFields()) {
							copyMapping = oldFieldMapping.getSourceToTargetMapByName(sourceField, targetField);

							if (copyMapping != null) {
								copyMapping.setSourceItem(sourceField);
								copyMapping.setTargetItem(targetField);

								newFieldMapping.addSourceToTargetMap(copyMapping);
							}
						}
					}
				}
			}
		}
	}

	public void saveCurrentState() {

	}

	public void revertToPreviousState() {

	}

	public void revertToNextState() {

	}

	public Mapping<Table> getTableToTableMapping() {
		return new Mapping<>(sourceDb.getTables(), cdmDb.getTables(), tableToTableMaps);
	}

	public Mapping<Field> getFieldToFieldMapping(Table sourceTable, Table targetTable) {
		ItemToItemMap key = new ItemToItemMap(sourceTable, targetTable);
		List<ItemToItemMap> fieldToFieldMaps = tableMapToFieldToFieldMaps.computeIfAbsent(key, k -> new ArrayList<>());
		return new Mapping<>(sourceTable.getFields(), targetTable.getFields(), fieldToFieldMaps);
	}

	public String getFilename() {
		return filename;
	}

	public void setTargetDatabase(Database targetDb) {
		this.cdmDb = targetDb;
	}

	public void setSourceDatabase(Database sourceDb) {
		this.sourceDb = sourceDb;
	}

	public Database getTargetDatabase() {
		return cdmDb;
	}

	public Database getSourceDatabase() {
		return sourceDb;
	}

	public void writeJson(OutputStream stream) throws IOException {
		Map<String, Object> args = new HashMap<>();
		args.put("PRETTY_PRINT", true);
		try (JsonWriter writer = new JsonWriter(stream, args)) {
			writer.write(this);
		} catch (JsonIoException ex) {
			throw (IOException)ex.getCause();
		}
	}

	public void save(String filename, FileFormat format) {
		try (FileOutputStream fileOutputStream = new FileOutputStream(filename)) {
			switch (format) {
				case Binary:
					try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream);
							ObjectOutputStream out = new ObjectOutputStream(gzipOutputStream)) {
						out.writeObject(this);
					}
					break;
				case GzipJson:
					try (GZIPOutputStream out = new GZIPOutputStream(fileOutputStream)) {
						writeJson(out);
					}
					break;
				case Json:
					writeJson(fileOutputStream);
					break;
			}
			this.filename = filename;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create an ETL instance by reading a saved file
	 *
	 * @param filename
	 *            path of the file
	 * @param format
	 *            [[FileFormat.Binary]] or [[FileFormat.Json]]
	 * @return resulting ETL object
	 */
	public static ETL fromFile(String filename, FileFormat format) {
		ETL result = null;
		try {
			switch (format) {
				case Binary:
					try (FileInputStream fileInputStream = new FileInputStream(filename);
							GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
							ObjectInputStream objectInputStream = new ObjectInputStream(gzipInputStream)) {
						result = (ETL) objectInputStream.readObject();
					}
					break;
				case GzipJson:
					try (FileInputStream fileInputStream = new FileInputStream(filename);
							GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
							JsonReader jr = new JsonReader(gzipInputStream)) {
						result = (ETL) jr.readObject();
					}
					break;
				case Json:
					try (FileInputStream fileInputStream = new FileInputStream(filename); JsonReader jr = new JsonReader(fileInputStream)) {
						result = (ETL) jr.readObject();
					}
					break;
			}
			result.filename = filename;
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		return result;
	}

	public void discardCounts() {
		for (Table table : sourceDb.getTables()) {
			for (Field field : table.getFields()) {
				field.setValueCounts(new ValueCounts());
			}
		}
	}

	public boolean hasStemTable() {
		return getSourceDatabase().getTables().stream().anyMatch(Table::isStem);
	}

	public List<String> getMappingsFromSourceField(Field sourceField) {
		List<String> result = new ArrayList<>();
		for (Map.Entry<ItemToItemMap, List<ItemToItemMap>> tableMapToFieldToField : tableMapToFieldToFieldMaps.entrySet()) {
			String targetTableName = tableMapToFieldToField.getKey().getTargetItem().getName();
			for (ItemToItemMap fieldToField : tableMapToFieldToField.getValue()) {
				if (fieldToField.getSourceItem() == sourceField) {
					String targetFieldName = fieldToField.getTargetItem().getName();
					result.add(String.format("%s.%s", targetTableName, targetFieldName));
				}
			}
		}
		return result;
	}

	public List<String> getMappingsToTargetField(Field targetField) {
		List<String> result = new ArrayList<>();
		for (Map.Entry<ItemToItemMap, List<ItemToItemMap>> tableMapToFieldToField : tableMapToFieldToFieldMaps.entrySet()) {
			String sourceTableName = tableMapToFieldToField.getKey().getSourceItem().getName();
			for (ItemToItemMap fieldToField : tableMapToFieldToField.getValue()) {
				if (fieldToField.getTargetItem() == targetField) {
					String sourceFieldName = fieldToField.getSourceItem().getName();
					result.add(String.format("%s.%s", sourceTableName, sourceFieldName));
				}
			}
		}
		return result;
	}
}
