package org.ohdsi.rabbitInAHat;

import org.ohdsi.rabbitInAHat.dataModel.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created 04-01-17.
 * Generates separate SQL files for each table to table mapping in the given directory.
 */
public class SQLGenerator {
    ETL etl;
    File outputDirectory;

    public SQLGenerator(ETL etl, String directory) {
        this.etl = etl;
        this.outputDirectory = new File(directory);
        if (!outputDirectory.exists()) {
            if (!outputDirectory.mkdir()) {
                throw new RuntimeException("SQL output directory " + directory + " could not be created.");
            }
        }
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
        Mapping<Field> fieldToFieldMapping = etl.getFieldToFieldMapping(sourceTable, targetTable);
        List<ItemToItemMap> mappings = fieldToFieldMapping.getSourceToTargetMapsOrderedByCdmItems();

        // Create new sql file in the selected directory
        File outFile = new File( outputDirectory, sourceTable.getName() + "_to_" + targetTable.getName() + ".sql");

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
            List<Field> targetsWithoutSource = new ArrayList<>();
            for (int i=0;i<n_mappings;i++) {
                ItemToItemMap mapping = mappings.get(i);

                // If mapping, then get this information.
                Field target;
                if (mapping == null) {
                    target = targetTable.getFieldByName(fieldToFieldMapping.getTargetItems().get(targetFieldsSeen.size()).getName());
                    targetsWithoutSource.add(target);
                } else {
                    target = targetTable.getFieldByName(mapping.getTargetItem().getName());
                }

                out.write("    ");
                out.write(target.getName());

                // Do not print comma if last is reached
                if (i != n_mappings-1) {
                    out.write(",");
                }

                // Warning if this target field has already been used
                if (!targetFieldsSeen.add(target)) {
                    out.write(createInLineComment("[!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED"));
                }

                out.write(createInLineComment(target.getComment()));

                out.write("\n");
            }

            // From
            out.write(")\n");
            out.write("SELECT\n");
            for (int i=0;i<n_mappings;i++) {
                ItemToItemMap mapping = mappings.get(i);

                String sourceName;
                String targetName;
                if (mapping == null) {
                    // Set value explicitly to null. Get target from the targetsWithoutSource
                    sourceName = "NULL";
                    targetName = targetsWithoutSource.remove(0).getName();
                    out.write(createInLineComment("[!WARNING!] no source column found. See possible comment at the INSERT INTO"));
                    out.write("\n");
                } else {
                    Field source = sourceTable.getFieldByName(mapping.getSourceItem().getName());
                    sourceName = sourceTable.getName() + "." + source.getName();
                    targetName = mapping.getTargetItem().getName();

                    out.write(createInLineComment("[VALUE   COMMENT]", source.getComment(), "\n"));
                    out.write(createInLineComment("[MAPPING   LOGIC]", mapping.getLogic(), "\n"));
                    out.write(createInLineComment("[MAPPING COMMENT]", mapping.getComment(), "\n"));
                }

                out.write("    ");
                out.write(sourceName);
                out.write(" AS ");
                out.write(targetName);

                // Do not print comma if last is reached
                if (i != n_mappings-1)
                    out.write(",");

                out.write("\n\n");
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

    private static String createInLineComment(String prefix, String input, String postfix) {
        if (input.trim().equals(""))
            return "";

        return String.format(" -- %s %s %s", prefix, input.replaceAll("\\s"," "), postfix);
    }

    private static String createBlockComment(String input) {
        if (input.trim().equals(""))
            return "";

        // Let new sentences span multiple lines
        input = input.replaceAll("\\.\\s","\n");

        return String.format("/*%n%s%n*/", input.trim());
    }
}
