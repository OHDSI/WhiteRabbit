package org.ohdsi.rabbitInAHat;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.ohdsi.ooxml.CustomXWPFDocument;
import org.ohdsi.rabbitInAHat.dataModel.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Maxim on 04-01-17.
 */
public class SQLGenerator {

    public static void generate(ETL etl, String filename) {
        File outFile = new File( filename );
        System.out.println( "Writing to: " + outFile.getAbsoluteFile() );

        // Simple test implementation. TBD
        for (Table targetTable : etl.getTargetDatabase().getTables()) {
            generateTableToTable(etl, targetTable);

        }


        // Read source id
//        try (FileOutputStream fout = new FileOutputStream(outFile);
//             OutputStreamWriter fwr = new OutputStreamWriter(fout, "UTF-8");
//             Writer out = new BufferedWriter(fwr)) {
//            String insertQuery = String.format("INSERT INTO %s () VALUES ();%n", targetTable.getName());
//                  out.write(insertQuery);
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

    private static void generateTableToTable(ETL etl, Table targetTable) {
        for (ItemToItemMap tableToTableMap : etl.getTableToTableMapping().getSourceToTargetMaps()) {
            if (tableToTableMap.getTargetItem() == targetTable) {
                System.out.println(targetTable.getName());
                Table sourceTable = (Table) tableToTableMap.getSourceItem();
                generateSqlFile(etl, sourceTable, targetTable);
            }
        }
    }

    private static void generateSqlFile(ETL etl, Table sourceTable, Table targetTable) {
        String sourceTableName = sourceTable.getName();
        String targetTableName = targetTable.getName();
        List<String> sources = new ArrayList<>();
        List<String> targets = new ArrayList<>();
        Mapping<Field> fieldtoFieldMapping = etl.getFieldToFieldMapping(sourceTable, targetTable);
        for (MappableItem targetField : fieldtoFieldMapping.getTargetItems()) {

            targets.add(targetField.getName());

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

//        File outFile = new File( filename + sourceTableName + targetTableName );
//        System.out.println( "Writing to: " + outFile.getAbsoluteFile() );

        System.out.println("INSERT INTO");
        System.out.println(targetTableName);
        System.out.println("(");
        for (String colName : targets) {
            System.out.println('\t'+colName);
        }
        System.out.println(")\nSELECT");
        for (String colName : sources) {
            System.out.println('\t'+colName);
        }
        System.out.println("FROM " + sourceTableName + ";");
    }
}
