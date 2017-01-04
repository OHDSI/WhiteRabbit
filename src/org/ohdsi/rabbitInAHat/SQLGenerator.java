package org.ohdsi.rabbitInAHat;

import org.ohdsi.rabbitInAHat.dataModel.*;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        // Generate a sql file for each source to target table mapping
        for (ItemToItemMap tableToTableMap : etl.getTableToTableMapping().getSourceToTargetMaps()) {
            writeSqlFile(tableToTableMap);
        }
    }

    private void writeSqlFile(ItemToItemMap tableToTableMap){
        Table sourceTable = (Table) tableToTableMap.getSourceItem();
        Table targetTable = (Table) tableToTableMap.getTargetItem();
        List<ItemToItemMap> mappings = etl.getFieldToFieldMapping(sourceTable, targetTable).getSourceToTargetMaps();

        // Create new sql file in the selected directory
        File outFile = new File( outputDirectory, sourceTable.getName() + "_to_" + targetTable.getName() + ".sql");
        System.out.println( "Writing to: " + outFile.getAbsoluteFile() );

        int n_mappings = mappings.size();
        Set<Field> targetFieldsSeen = new HashSet<>();
        try (FileOutputStream fout = new FileOutputStream(outFile);
             OutputStreamWriter fwr = new OutputStreamWriter(fout, "UTF-8");
             Writer out = new BufferedWriter(fwr)) {

            // Table specific comments
            out.write(createBlockComment(sourceTable.getComment()));
            out.write(createBlockComment(targetTable.getComment()));
            out.write(createBlockComment(tableToTableMap.getComment()));
            out.write(createBlockComment(tableToTableMap.getLogic()));
            out.write('\n');

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

                // Warning if this target field has already been used
                if (!targetFieldsSeen.add(target)) {
                    out.write(createInLineComment("!#WARNING!# FIELD ALREADY USED"));
                }

                out.write(createInLineComment(target.getComment()));

                out.write("\n");
            }

            // From
            out.write(")\n");
            out.write("SELECT\n");
            for (int i=0;i<n_mappings;i++) {
                ItemToItemMap mapping = mappings.get(i);
                Field source = sourceTable.getFieldByName(mapping.getSourceItem().getName());
                out.write('\t');
                out.write(source.getName());

                // Do not print comma if last is reached
                if (i != n_mappings-1) {
                    out.write(",");
                }

                out.write(createInLineComment(source.getComment()));
                out.write(createInLineComment(mapping.getComment()));
                out.write(createInLineComment(mapping.getLogic()));

                out.write("\n");
            }
            out.write("FROM " + sourceTable.getName());
            out.write("\n;");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String createInLineComment(String input) {
        if (input.trim().equals(""))
            return "";

        return " -- " + input.replaceAll("\\s"," ");
    }

    private static String createBlockComment(String input) {
        if (input.trim().equals(""))
            return "";

        // Let new sentences span multiple lines
        input = input.replaceAll("\\.\\s","\n");

        return String.format("/*%n%s%n*/", input.trim());
    }
}
