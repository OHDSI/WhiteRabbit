package org.ohdsi.whiterabbit.scan;

import org.ohdsi.databases.DbType;
import org.ohdsi.ooxml.ReadXlsxFileWithHeader;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.RowUtilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ScanTestUtils {
    public static void verifyScanResultsFromXSLX(Path results, DbType dbType) {
        assertTrue(Files.exists(results));

        FileInputStream file = null;
        try {
            file = new FileInputStream(new File(results.toUri()));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(String.format("File %s was expected to be found, but does not exist.", results), e);
        }

        ReadXlsxFileWithHeader sheet = new ReadXlsxFileWithHeader(file);

        List<Row> data = new ArrayList<>();
        int i = 0;
        for (Row row : sheet) {
            data.add(row);
            i++;
        }

        // apparently the order of rows in the generated xslx table is not fixed,
        // so they need to be sorted to be able to verify their contents
        RowUtilities.sort(data, "Table", "Field");
        assertEquals(42, i);

        // since the table is generated with empty lines between the different tables of the source database,
        // a number of empty lines is expected. Verify this, and the first non-empty line
        expectRowNIsLike(0, data, dbType, "", "", "", "", "", "");
        expectRowNIsLike(1, data, dbType, "", "", "", "", "", "");
        expectRowNIsLike(2, data, dbType, "cost", "amount_allowed", "", "numeric", "0", "34");

        // sample some other rows in the available range
        expectRowNIsLike(9, data,dbType, "cost", "drg_source_value", "", "character varying", "0", "34");
        expectRowNIsLike(23, data,dbType, "cost", "total_paid", "", "numeric", "0", "34");
        expectRowNIsLike(24, data,dbType, "person", "birth_datetime", "", "timestamp without time zone", "0", "30");
        expectRowNIsLike(41, data,dbType, "person", "year_of_birth", "", "integer", "0", "30");
    }

    private static void expectRowNIsLike(int n, List<Row> rows, DbType dbType, String... expectedValues) {
        assert expectedValues.length == 6;
        testColumnValue(n, rows.get(n), "Table", expectedValues[0]);
        testColumnValue(n, rows.get(n), "Field", expectedValues[1]);
        testColumnValue(n, rows.get(n), "Description", expectedValues[2]);
        testColumnValue(n, rows.get(n), "Type", expectedTypeValue(expectedValues[3], dbType));
        testColumnValue(n, rows.get(n), "Max length", expectedValues[4]);
        testColumnValue(n, rows.get(n), "N rows", expectedValues[5]);
    }

    private static void testColumnValue(int i, Row row, String fieldName, String expected) {
        if (!expected.equalsIgnoreCase(row.get(fieldName))) {
            fail(String.format("In row %d, value '%s' was expected for column '%s', but '%s' was found",
                    i, expected, fieldName, row.get(fieldName)));
        }
    }

    private static String expectedTypeValue(String columnName, DbType dbType) {
        /*
         * This is very pragmatical and may need to change when tests are added for more databases.
         * For now, PostgreSQL is used as the reference, and the expected types need to be adapted to match
         * for other database.
         */
        if (dbType == DbType.POSTGRESQL || columnName.equals("")) {
            return columnName;
        }
        else if (dbType == DbType.ORACLE){
            switch (columnName) {
                case "integer":
                    return "NUMBER";
                case "numeric":
                    return "FLOAT";
                case "character varying":
                    return "VARCHAR2";
                case "timestamp without time zone":
                    // seems a mismatch in the OMOP CMD v5.2 (Oracle defaults to WITH time zone)
                    return "TIMESTAMP(6) WITH TIME ZONE";
                default:
                    throw new RuntimeException("Unsupported column type: " + columnName);
            }
        }
        else {
            throw new RuntimeException("Unsupported DBType: " + dbType);
        }
    }
}
