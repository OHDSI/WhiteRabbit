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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteCSVFileWithHeader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ETLSummaryGenerator {

    public static void generateSourceFieldListCsv(ETL etl, String filename) {
        if (!filename.toLowerCase().endsWith(".csv"))
            filename = filename + ".csv";

        System.out.println("Generating source field list " + filename);
        // TODO: try with resources
        WriteCSVFileWithHeader out = new WriteCSVFileWithHeader(filename, CSVFormat.RFC4180);
        for (Row row : createSourceFieldList(etl)) {
            out.write(row);
        }
        out.close();
    }

    public static void generateSourceFieldListExcel(ETL etl, String filename) {
        System.out.println("Generating source mappings");

        SXSSFWorkbook workbook = new SXSSFWorkbook(100); // keep 100 rows in memory, exceeding rows will be flushed to disk

        // Create overview sheet
        Sheet sheet = workbook.createSheet("All source fields");
//        org.apache.poi.ss.usermodel.Row headerRow = sheet.createRow(0);
//        addRow(sheet,  "Source Table", "Source Field", "Description", "Mapped?", "Number of mappings", "Mappings");
        for (Row row : createSourceFieldList(etl)) {
            org.apache.poi.ss.usermodel.Row excelRow = sheet.createRow(sheet.getPhysicalNumberOfRows());
            row.getCells().forEach(value -> {
                Cell cell = excelRow.createCell(excelRow.getPhysicalNumberOfCells());
                cell.setCellValue(value);
            });
        }

        try {
            FileOutputStream out = new FileOutputStream(new File(filename));
            workbook.write(out);
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private static List<Row> createSourceFieldList(ETL etl) {
        List<Row> rows = new ArrayList<>();

        for (Table sourceTable : etl.getSourceDatabase().getTables()) {
            for (Field sourceField : sourceTable.getFields()) {
                List<String> fieldMappings = etl.getMappingsforSourceField(sourceField);
                int nMappings = fieldMappings.size();
                Row row = new Row();
                row.add("Source Table", sourceTable.getName());
                row.add("Source Field", sourceField.getName());
                row.add("Description", sourceField.getComment());
                row.add("Mapped?", nMappings > 0 ? "X" : "");
                row.add("Number of mappings", nMappings > 0 ? String.valueOf(nMappings) : "");
                row.add("Mappings", String.join(",", fieldMappings));
                rows.add(row);
            }
        }
        return rows;
    }
}
