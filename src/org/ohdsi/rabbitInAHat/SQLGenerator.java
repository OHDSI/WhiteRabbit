package org.ohdsi.rabbitInAHat;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.ohdsi.ooxml.CustomXWPFDocument;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;
import org.ohdsi.rabbitInAHat.dataModel.Table;

import java.io.*;

/**
 * Created by Maxim on 04-01-17.
 */
public class SQLGenerator {

    public static void generate(ETL etl, String filename) {
        File outFile = new File( filename );
        System.out.println( "Writing to: " + outFile.getAbsoluteFile() );

        // Read source id
        try (FileOutputStream fout = new FileOutputStream(outFile);
             OutputStreamWriter fwr = new OutputStreamWriter(fout, "UTF-8");
             Writer out = new BufferedWriter(fwr)) {

            // Simple test implementation. TBD
            for (Table targetTable : etl.getTargetDatabase().getTables()) {
                String insertQuery = String.format("INSERT INTO %s () VALUES ();%n", targetTable.getName());
                out.write(insertQuery);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
