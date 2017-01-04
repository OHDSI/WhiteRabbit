package org.ohdsi.rabbitInAHat;

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
        // Generate a sql file for each source table to target table mapping
        for (Table targetTable : etl.getTargetDatabase().getTables()) {
            for (ItemToItemMap tableToTableMap : etl.getTableToTableMapping().getSourceToTargetMaps()) {
                if (tableToTableMap.getTargetItem() == targetTable) {
                    Table sourceTable = (Table) tableToTableMap.getSourceItem();
                    generateSqlFile(sourceTable, targetTable);
                }
            }
        }
    }

    private void generateSqlFile(Table sourceTable, Table targetTable) {
        // Get all source to target field mappings
        List<ItemToItemMap> mappings = new ArrayList<>();
        Mapping<Field> fieldtoFieldMapping = etl.getFieldToFieldMapping(sourceTable, targetTable);
        for (MappableItem targetField : fieldtoFieldMapping.getTargetItems()) {
            for (ItemToItemMap fieldToFieldMap : fieldtoFieldMapping.getSourceToTargetMaps()) {
                if (fieldToFieldMap.getTargetItem() == targetField) {
                    // Add target source pair
                    mappings.add(fieldToFieldMap);
                    // TODO: handle many sources to one target
                }
            }
        }

        writeSqlFile(sourceTable, targetTable, mappings);
    }

    private void writeSqlFile(Table sourceTable, Table targetTable, List<ItemToItemMap> mappings) {
        // Create new sql file in the selected directory
        File outFile = new File( outputDirectory, sourceTable.getName() + "_to_" + targetTable.getName() + ".sql");
        System.out.println( "Writing to: " + outFile.getAbsoluteFile() );

        int n_mappings = mappings.size();

        try (FileOutputStream fout = new FileOutputStream(outFile);
             OutputStreamWriter fwr = new OutputStreamWriter(fout, "UTF-8");
             Writer out = new BufferedWriter(fwr)) {

            // Table specific comments
            createBlockComment(sourceTable.getComment() + "\n" + targetTable.getComment());
            // TODO: table to table logic

            // To
            out.write("INSERT INTO " + targetTable.getName() + "\n");
            out.write("(\n");
            for (int i=0;i<n_mappings;i++) {
                ItemToItemMap mapping = mappings.get(i);
                Field target = targetTable.getFieldByName(mapping.getTargetItem().getName());
                out.write('\t');
                out.write(target.getName());

                // Do not print comma if last is reached
                if (i != n_mappings-1) {
                    out.write(",");
                }

                out.write(createOneLineComment(target.getComment()));

                out.write("\n");
            }

            // From
            out.write(")\nSELECT\n");
            for (int i=0;i<n_mappings;i++) {
                ItemToItemMap mapping = mappings.get(i);
                Field source = sourceTable.getFieldByName(mapping.getSourceItem().getName());
                out.write('\t');
                out.write(source.getName());

                // Do not print comma if last is reached
                if (i != n_mappings-1) {
                    out.write(",");
                }

                out.write(createOneLineComment(source.getComment()));
                out.write(createOneLineComment(mapping.getComment()));
                out.write(createOneLineComment(mapping.getLogic()));

                out.write("\n");
            }
            out.write("FROM " + sourceTable.getName() + ";");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String createOneLineComment(String input) {
        if (input.trim().equals(""))
            return "";

        return "-- " + input.replaceAll("\\s"," ");
    }

    private static String createBlockComment(String input) {
        if (input.trim().equals(""))
            return "";

        return String.format("/*%n%s%n*/", input);
    }
}
