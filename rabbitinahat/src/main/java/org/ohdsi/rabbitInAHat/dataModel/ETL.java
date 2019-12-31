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
package org.ohdsi.rabbitInAHat.dataModel;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.cedarsoftware.util.io.JsonReader;
import com.cedarsoftware.util.io.JsonWriter;

public class ETL implements Serializable {
	public enum FileFormat {
		Binary, Json, GzipJson
	}

	private Database								sourceDb					= new Database();
	private Database								cdmDb						= new Database();
	private List<ItemToItemMap>						tableToTableMaps			= new ArrayList<ItemToItemMap>();
	private Map<ItemToItemMap, List<ItemToItemMap>>	tableMapToFieldToFieldMaps	= new HashMap<ItemToItemMap, List<ItemToItemMap>>();
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
		return new Mapping<Table>(sourceDb.getTables(), cdmDb.getTables(), tableToTableMaps);
	}

	public Mapping<Field> getFieldToFieldMapping(Table sourceTable, Table targetTable) {
		List<ItemToItemMap> fieldToFieldMaps = tableMapToFieldToFieldMaps.get(new ItemToItemMap(sourceTable, targetTable));
		if (fieldToFieldMaps == null) {
			fieldToFieldMaps = new ArrayList<ItemToItemMap>();
			tableMapToFieldToFieldMaps.put(new ItemToItemMap(sourceTable, targetTable), fieldToFieldMaps);
		}
		return new Mapping<Field>(sourceTable.getFields(), targetTable.getFields(), fieldToFieldMaps);
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

	public String toJson() {
		return JsonWriter.formatJson(JsonWriter.objectToJson(this));
	}

	public void save(String filename, FileFormat format) {
		try {
			switch (format) {
				case Binary:
					try (FileOutputStream fileOutputStream = new FileOutputStream(filename);
							GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream);
							ObjectOutputStream out = new ObjectOutputStream(gzipOutputStream)) {
						out.writeObject(this);
					}
					break;
				case GzipJson:
					String json = toJson();
					try (FileOutputStream fileOutputStream = new FileOutputStream(filename);
							GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream);
							OutputStreamWriter out = new OutputStreamWriter(gzipOutputStream, "UTF-8")) {
						out.write(json);
					}
					break;
				case Json:
					String json2 = toJson();
					try (FileOutputStream fileOutputStream = new FileOutputStream(filename);
							OutputStreamWriter out = new OutputStreamWriter(fileOutputStream, "UTF-8")) {
						out.write(json2);
					}
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
		for (Table table : sourceDb.getTables())
			for (Field field : table.getFields())
				field.setValueCounts(new String[0][]);
	}

	public boolean hasStemTable() {
		return getSourceDatabase().getTables().stream().anyMatch(Table::isStem);
	}
}
