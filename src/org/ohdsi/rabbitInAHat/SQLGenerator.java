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
            generateTableToTable(targetTable);
        }
    }

    private void generateTableToTable(Table targetTable) {
        for (ItemToItemMap tableToTableMap : etl.getTableToTableMapping().getSourceToTargetMaps()) {
            if (tableToTableMap.getTargetItem() == targetTable) {
                Table sourceTable = (Table) tableToTableMap.getSourceItem();
                writeSqlFile(sourceTable, targetTable);
            }
        }
    }

    private void writeSqlFile(Table sourceTable, Table targetTable) {
        String sourceTableName = sourceTable.getName();
        String targetTableName = targetTable.getName();
        List<ItemToItemMap> sources = new ArrayList<>();
        List<Field> targets = new ArrayList<>();
        Mapping<Field> fieldtoFieldMapping = etl.getFieldToFieldMapping(sourceTable, targetTable);
        for (MappableItem targetField : fieldtoFieldMapping.getTargetItems()) {
            for (ItemToItemMap fieldToFieldMap : fieldtoFieldMapping.getSourceToTargetMaps()) {
                if (fieldToFieldMap.getTargetItem() == targetField) {
                    // Add target source pair
                    targets.add((Field) targetField);
                    sources.add(fieldToFieldMap);
                    break; // TODO; what if a many to one mapping?
                }
            }
        }

        // Write sql file to the selected directory
        File outFile = new File( outputDirectory, sourceTableName + "_to_" + targetTableName + ".sql");
        System.out.println( "Writing to: " + outFile.getAbsoluteFile() );

        try (FileOutputStream fout = new FileOutputStream(outFile);
             OutputStreamWriter fwr = new OutputStreamWriter(fout, "UTF-8");
             Writer out = new BufferedWriter(fwr)) {

            // To
            out.write("INSERT INTO " + targetTableName + "\n");
            out.write("(\n");
            for (int i=0;i<targets.size();i++) {
                Field target = targets.get(i);
                out.write('\t');
                out.write(target.getName());
                // Do not print comma if last is reached
                if (i < targets.size()-1) {
                    out.write(",");
                }

                out.write(createComment(target.getComment()));

                out.write("\n");
            }

            // From
            out.write(")\nSELECT\n");
            for (int i=0;i<sources.size();i++) {
                ItemToItemMap source = sources.get(i);
                out.write('\t');
                out.write(source.getSourceItem().getName());
                // Do not print comma if last is reached
                if (i < targets.size()-1) {
                    out.write(",");
                }

                out.write(createComment(source.getComment()));
                out.write(createComment(source.getLogic()));

                out.write("\n");
            }
            out.write("FROM " + sourceTableName + ";");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String createComment(String input) {
        if (input.equals(""))
            return "";

        return "-- " + input.replaceAll("\\s"," ");
    }

}
