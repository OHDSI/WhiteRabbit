package org.ohdsi.rabbitInAHat;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.ohdsi.ooxml.CustomXWPFDocument;
import org.ohdsi.rabbitInAHat.dataModel.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created 04-01-17.
 * Generates separate SQL files for each table to table mapping in the given directory.
 */
public class SQLGenerator {
    ETL etl;
    String outputDirectory;

    public SQLGenerator(ETL etl, String directory) {
        this.etl = etl;
        this.outputDirectory = directory;
    }

    public void generate() {
        // Simple test implementation. TBD
        for (Table targetTable : etl.getTargetDatabase().getTables()) {
            generateTableToTable(etl, targetTable);

        }
    }

    private void generateTableToTable(ETL etl, Table targetTable) {
        for (ItemToItemMap tableToTableMap : etl.getTableToTableMapping().getSourceToTargetMaps()) {
            if (tableToTableMap.getTargetItem() == targetTable) {
                System.out.println(targetTable.getName());
                Table sourceTable = (Table) tableToTableMap.getSourceItem();
                generateSqlFile(etl, sourceTable, targetTable);
            }
        }
    }

    private void generateSqlFile(ETL etl, Table sourceTable, Table targetTable) {
        String sourceTableName = sourceTable.getName();
        String targetTableName = targetTable.getName();
        List<String> sources = new ArrayList<>();
        List<String> targets = new ArrayList<>();
        Mapping<Field> fieldtoFieldMapping = etl.getFieldToFieldMapping(sourceTable, targetTable);
        for (MappableItem targetField : fieldtoFieldMapping.getTargetItems()) {
            StringBuilder source = new StringBuilder();
            StringBuilder logic = new StringBuilder();
            StringBuilder comment = new StringBuilder();
            for (ItemToItemMap fieldToFieldMap : fieldtoFieldMapping.getSourceToTargetMaps()) {
                if (fieldToFieldMap.getTargetItem() == targetField) {
                    if (source.length() != 0)
                        source.append("\n");
                    source.append(fieldToFieldMap.getSourceItem().getName().trim());

                    if (logic.length() != 0)
                        logic.append("\n");
                    logic.append(fieldToFieldMap.getLogic().trim());

                    if (comment.length() != 0)
                        comment.append("\n");
                    comment.append(fieldToFieldMap.getComment().trim());

                    // Add target source pair
                    targets.add(targetField.getName());
                    sources.add(source.toString());
                    break; // TODO; what if a many to one mapping?
                }
            }

            for (Field field : targetTable.getFields()) {
                if (field.getName().equals(targetField.getName())) {
                    if (comment.length() != 0)
                        comment.append("\n");
                    comment.append(field.getComment().trim());
                }
            }
        }

        // Write sql
        File outFile = new File( outputDirectory, sourceTableName + "_to_" + targetTableName + ".sql");
        System.out.println( "Writing to: " + outFile.getAbsoluteFile() );

        try (FileOutputStream fout = new FileOutputStream(outFile);
             OutputStreamWriter fwr = new OutputStreamWriter(fout, "UTF-8");
             Writer out = new BufferedWriter(fwr)) {

            out.write("INSERT INTO " + targetTableName + "\n");

            out.write("(\n");
            for (String colName : targets) {
                out.write('\t'+colName + ",\n"); // TODO: remove extra comma at the end.
            }

            out.write(")\nSELECT\n");
            for (String colName : sources) {
                out.write('\t'+colName + ",\n"); // TODO: remove extra comma at the end.
            }

            out.write("FROM " + sourceTableName + ";");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
