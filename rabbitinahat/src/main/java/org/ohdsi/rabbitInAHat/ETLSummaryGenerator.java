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
package org.ohdsi.rabbitInAHat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.ohdsi.rabbitInAHat.dataModel.*;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteCSVFileWithHeader;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ETLSummaryGenerator {

    static void writeCsv(String filename, List<Row> rows) {
        if (!filename.toLowerCase().endsWith(".csv")) {
            filename = filename + ".csv";
        }

        // TODO: try with resources
        WriteCSVFileWithHeader out = new WriteCSVFileWithHeader(filename, CSVFormat.RFC4180);
        for (Row row : rows) {
            out.write(row);
        }
        out.close();
    }

    static void generateSourceFieldListCsv(ETL etl, String filename) {
        writeCsv(filename, createSourceFieldList(etl));
    }

    private static List<Row> createSourceFieldList(ETL etl) {
        List<Row> rows = new ArrayList<>();

        for (Table sourceTable : etl.getSourceDatabase().getTables()) {
            for (Field sourceField : sourceTable.getFields()) {
                List<String> fieldMappings = etl.getMappingsFromSourceField(sourceField);
                int nMappings = fieldMappings.size();
                Row row = new Row();
                row.add("Source Table", sourceTable.getName());
                row.add("Source Field", sourceField.getName());
                row.add("Type", sourceField.getType());
                row.add("Comment", sourceField.getComment());
                row.add("Mapped?", nMappings > 0 ? "X" : "");
                row.add("Number of mappings", nMappings > 0 ? String.valueOf(nMappings) : "");
                row.add("Mappings", String.join(",", fieldMappings));
                rows.add(row);
            }
        }
        return rows;
    }

    static void generateTargetFieldListCsv(ETL etl, String filename) {
        writeCsv(filename, createTargetFieldList(etl));
    }

    private static List<Row> createTargetFieldList(ETL etl) {
        List<Row> rows = new ArrayList<>();

        for (Table targetTable : etl.getTargetDatabase().getTables()) {
            for (Field targetField : targetTable.getFields()) {
                List<String> fieldMappings = etl.getMappingsToTargetField(targetField);
                int nMappings = fieldMappings.size();
                Row row = new Row();
                row.add("Target Table", targetTable.getName());
                row.add("Target Field", targetField.getName());
                row.add("Required?", targetField.isNullable() ? "" : "*");
                row.add("Comment", targetField.getComment());
                row.add("Mapped?", nMappings > 0 ? "X" : "");
                row.add("Number of mappings", nMappings > 0 ? String.valueOf(nMappings) : "");
                row.add("Mappings", String.join(",", fieldMappings));
                rows.add(row);
            }
        }
        return rows;
    }

    static void generateTableMappingsCsv(ETL etl, String filename) {
        writeCsv(filename, createTableMappingList(etl));
    }

    private static List<Row> createTableMappingList(ETL etl) {
        List<Row> rows = new ArrayList<>();

        List<ItemToItemMap> tableToTableMaps = etl.getTableToTableMapping().getSourceToTargetMaps();
        for (ItemToItemMap tableToTableMap : tableToTableMaps) {
            Table sourceTable = (Table) tableToTableMap.getSourceItem();
            Table targetTable = (Table) tableToTableMap.getTargetItem();
            Row row = new Row();
            row.add("Source Table", sourceTable.getName());
            row.add("Target Table", targetTable.getName());
            row.add("Comment", tableToTableMap.getComment());
            row.add("Logic", tableToTableMap.getLogic());

            List<ItemToItemMap> fieldToFieldMaps = etl.getFieldToFieldMapping(sourceTable, targetTable).getSourceToTargetMaps();
            row.add("Number of field mappings", fieldToFieldMaps.size());
            rows.add(row);
        }

        return rows;
    }
}
