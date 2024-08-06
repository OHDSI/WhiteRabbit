package org.ohdsi.rabbitInAHat.dataModel;

import org.apache.commons.io.input.BOMInputStream;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

class TestDatabase {

    @Test
    void testGenerateModelFromCSV() throws IOException {
        // confirm that issue #411 is fixed, can read custom models from (UTF-8) CSV files with and without BOM

        // generate a model from a CSV file without BOM
        String testFileWithoutBom = "tiny_riah_without_bom.csv";
        InputStream inWithoutBom = TestDatabase.class.getResourceAsStream(testFileWithoutBom);
        assertNotNull(inWithoutBom);
        Database ignoredWithoutBom = Database.generateModelFromCSV(inWithoutBom, testFileWithoutBom);

        // generate a model from a CSV file with BOM
        String testFileWithBom = "tiny_riah_with_bom.csv";
        InputStream inWithBom = TestDatabase.class.getResourceAsStream(testFileWithBom);
        assertNotNull(inWithBom);
        Database ignoredWithBom = Database.generateModelFromCSV(inWithBom, testFileWithBom);

    }
}