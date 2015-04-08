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
 ******************************************************************************/
package org.ohdsi.rabbitInAHat.dataModel;

import com.cedarsoftware.util.io.JsonReader;
import com.cedarsoftware.util.io.JsonWriter;
import com.cedarsoftware.util.io.MetaUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ETL implements Serializable {
    public enum FileFormat {
        Binary,
        Json
    }

    private Database sourceDb = new Database();
    private Database cdmDb = new Database();
    private List<ItemToItemMap> tableToTableMaps = new ArrayList<ItemToItemMap>();
    private Map<ItemToItemMap, List<ItemToItemMap>> tableMapToFieldToFieldMaps = new HashMap<ItemToItemMap, List<ItemToItemMap>>();
    private transient String filename = null;
    private static final long serialVersionUID = 8987388381751618498L;

    public void saveCurrentState() {

    }

    public void revertToPreviousState() {

    }

    public void revertToNextState() {

    }

    public Mapping<Table> getTableToTableMapping() {
        return new Mapping<Table>(sourceDb.getTables(), cdmDb.getTables(), tableToTableMaps);
    }

    public Mapping<Field> getFieldToFieldMapping(Table sourceTable, Table cdmTable) {
        List<ItemToItemMap> fieldToFieldMaps = tableMapToFieldToFieldMaps.get(new ItemToItemMap(sourceTable, cdmTable));
        if (fieldToFieldMaps == null) {
            fieldToFieldMaps = new ArrayList<ItemToItemMap>();
            tableMapToFieldToFieldMaps.put(new ItemToItemMap(sourceTable, cdmTable), fieldToFieldMaps);
        }
        return new Mapping<Field>(sourceTable.getFields(), cdmTable.getFields(), fieldToFieldMaps);
    }

    public String getFilename() { return filename; }

    public void setCDMDatabase(Database cdmDb) {
        this.cdmDb = cdmDb;
    }

    public void setSourceDatabase(Database sourceDb) {
        this.sourceDb = sourceDb;
    }

    public Database getCDMDatabase() {
        return cdmDb;
    }

    public Database getSourceDatabase() {
        return sourceDb;
    }

    public ItemToItemMap createItemToItemMap(MappableItem source, MappableItem target) {
        ItemToItemMap itemToItemMap = new ItemToItemMap(source, target);
        if (source instanceof Table) {
            tableToTableMaps.add(itemToItemMap);
        } else {
            tableMapToFieldToFieldMaps.get(((Field) source).getTable()).add(itemToItemMap);
        }
        return itemToItemMap;
    }

    /**
     * Convert into pretty-print JSON
     * @param includeCounts if false, exclude the valueCounts field from result
     * @return JSON representation of object
     */
    public String toJson(boolean includeCounts) {
        Map<String, Object> args = new HashMap<>();
        if (!includeCounts) {
            Map<Class, List<String>> fieldSpecifier = new HashMap<>();
            ArrayList<java.lang.reflect.Field> fieldFields =
                    new ArrayList<>(MetaUtils.getDeepDeclaredFields(Field.class).values());
            ArrayList<String> fieldNames = new ArrayList<>();
            for (java.lang.reflect.Field field : fieldFields) {
                String fieldName = field.getName();
                if (Objects.equals(fieldName, "valueCounts")) continue;
                fieldNames.add(fieldName);
            }
            fieldSpecifier.put(Field.class, fieldNames);
            args.put(JsonWriter.FIELD_SPECIFIERS, fieldSpecifier);
        }
        return JsonWriter.formatJson(JsonWriter.objectToJson(this, args));
    }

    public String toJson() {
        return toJson(true);
    }

    public void save(String filename, FileFormat format, boolean includeCounts) {
        try {
            switch (format) {
                case Binary:
                    try (FileOutputStream fileOutputStream = new FileOutputStream(filename);
                         GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream);
                         ObjectOutputStream out = new ObjectOutputStream(gzipOutputStream))
                    {
                        out.writeObject(this);
                    }
                    break;
                case Json:
                    String json = toJson(includeCounts);
                    Files.write(Paths.get(filename), json.getBytes("utf-8"));
                    break;
            }
            this.filename = filename;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create an ETL instance by reading a saved file
     * @param filename path of the file
     * @param format [[FileFormat.Binary]] or [[FileFormat.Json]]
     * @return resulting ETL object
     */
    public static ETL FromFile(String filename, FileFormat format) {
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
                case Json:
                    try (FileInputStream inputStream = new FileInputStream(filename);
                         JsonReader jr = new JsonReader(inputStream)) {
                        result = (ETL)jr.readObject();
                    }
                    break;
            }
            result.filename = filename;
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return result;
    }
}
