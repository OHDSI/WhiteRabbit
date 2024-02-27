/*******************************************************************************
 * Copyright 2023 Observational Health Data Sciences and Informatics & The Hyve
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
package org.ohdsi.whiterabbit.scan;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.assertj.swing.timing.Condition;
import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.whiterabbit.Console;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.swing.timing.Pause.pause;
import static org.assertj.swing.timing.Timeout.timeout;
import static org.junit.jupiter.api.Assertions.*;
import static org.ohdsi.databases.configuration.DbType.*;

public class ScanTestUtils {

    static Logger logger = LoggerFactory.getLogger(ScanTestUtils.class);


    // Convenience for having the same scan parameters across tests
    public static SourceDataScan createSourceDataScan() {
        SourceDataScan sourceDataScan = new SourceDataScan();
        sourceDataScan.setMinCellCount(5);
        sourceDataScan.setScanValues(true);
        sourceDataScan.setMaxValues(1000);
        sourceDataScan.setNumStatsSamplerSize(0);
        sourceDataScan.setCalculateNumericStats(false);
        sourceDataScan.setSampleSize(100000);

        return sourceDataScan;
    }

    public static boolean scanResultsSheetMatchesReference(Path scanResults, Path referenceResults, DbType dbType) throws IOException {
        Map<String, List<List<String>>> scanSheets = readXlsxAsStringValues(scanResults);
        Map<String, List<List<String>>> referenceSheets = readXlsxAsStringValues(referenceResults);

        return scanValuesMatchReferenceValues(scanSheets, referenceSheets, dbType);
    }

    public static boolean isScanReportGeneratedAndMatchesReference(Console console, Path expectedPath, Path referencePath, DbType dbType) throws IOException {
        assertNotNull(console);
        // wait for the "Scan report generated:" message in the Console text area
        pause(new Condition("Label Timeout") {
            public boolean test() {
                return console.getText().contains("Scan report generated:");
            }

        }, timeout(10000));
        assertTrue(console.getText().contains(expectedPath.toString()));

        return scanResultsSheetMatchesReference(expectedPath, referencePath, dbType);
    }

    public static <scannedData> boolean scanValuesMatchReferenceValues(Map<String, List<List<String>>> scanSheets, Map<String, List<List<String>>> referenceSheets, DbType dbType) {
        assertEquals(referenceSheets.size(), scanSheets.size(), "Number of sheets does not match.");

        List<String> tabNames = new ArrayList<>(referenceSheets.keySet());
        for (String tabName: tabNames) {
            if (scanSheets.containsKey(tabName)) {
                List<List<String>> scanSheet = scanSheets.get(tabName);
                List<List<String>> referenceSheet = referenceSheets.get(tabName);
                assertEquals(scanSheet.size(), referenceSheet.size(), String.format("Number of rows in sheet %s does not match.", tabName));
                // in WhiteRabbit v0.10.7 and earlier, the order of tables is not defined, so this can result in differences due to the rows
                // being in a different order. By sorting the rows in both sheets, these kind of differences should not play a role.
                if (tabName.equalsIgnoreCase("Field Overview") || tabName.equalsIgnoreCase("Table Overview")) {
                    scanSheet.sort(new ColumnValueComparator());
                    referenceSheet.sort(new ColumnValueComparator());
                } else if (!tabName.equals("_")) {
                    scanSheet = transposeAndSort(scanSheet);
                    referenceSheet = transposeAndSort(referenceSheet);
                }

                final List<List<String>> scannedData = scanSheet;
                final List<List<String>> referenceData = referenceSheet;

                for (int i = 0; i < scanSheet.size(); ++i) {
                    AtomicInteger mismatches = new AtomicInteger(0);
                    final int fi = i;
                    IntStream.range(0, scanSheet.get(fi).size())
                            .parallel()
                            .forEach(j -> {
                                final String scanValue = scannedData.get(fi).get(j);
                                final String referenceValue = referenceData.get(fi).get(j);
                                if (!isExcludedFromMatching(tabName, fi, scanValue, referenceValue, dbType)) {
                                    if (tabName.equals("Field Overview") && j == 3 && !scanValue.equalsIgnoreCase(referenceValue)) {
                                        if (!matchTypeName(scanValue, referenceValue, dbType)) {
                                            mismatches.incrementAndGet();
                                            logger.error(String.format("Field type '%s' cannot be matched with reference type '%s' for DbType %s",
                                                    scanValue, referenceValue, dbType.name()));
                                        }
                                    } else {
                                        if (!scanValue.equalsIgnoreCase(referenceValue) &&
                                            !isAcceptedDifference(scannedData, referenceData, fi, j, dbType)) {
                                            mismatches.incrementAndGet();
                                            logger.error(
                                                    String.format("In sheet %s, value '%s' in scan results does not match '%s' in reference " +
                                                                    "(row %s, column %s, data col0='%s', data col1='%s', ref col0='%s', ref col1='%s')",
                                                            tabName, scanValue, referenceValue, fi, j,
                                                            scannedData.get(fi).get(0), scannedData.get(fi).get(1),
                                                            referenceData.get(fi).get(0), referenceData.get(fi).get(1)));
                                        }
                                    }
                                }
                            });
                    assertEquals(0, mismatches.get(), "No mismatches of values with the reference data should have occurred");
                }
            }
        }

        return true;
    }

    private static boolean isAcceptedDifference(List<List<String>> scannedData, List<List<String>> referenceData, int row, int column, DbType dbType) {
        if (dbType == SAS7BDAT) {
            // row 98, column 4, data col0='test-columnar.sas7bdat', data col1='date'
            if (row == 98 && column == 4 &&
                    scannedData.get(row).get(0).equalsIgnoreCase("test-columnar.sas7bdat") &&
                    referenceData.get(row).get(0).equalsIgnoreCase("test-columnar.sas7bdat") &&
                    scannedData.get(row).get(1).equalsIgnoreCase("date") &&
                    referenceData.get(row).get(1).equalsIgnoreCase("date") &&
                    scannedData.get(row).get(column).equals("28.0") &&
                    referenceData.get(row).get(column).equals("29.0")
            ) {
                // this is a knopwn difference that will not show up in a dev environment, but it
                // does show up in Github actions
                return true;
            }
            if (row == 99 && column == 4 &&
                    scannedData.get(row).get(0).equalsIgnoreCase("test-columnar.sas7bdat") &&
                    referenceData.get(row).get(0).equalsIgnoreCase("test-columnar.sas7bdat") &&
                    scannedData.get(row).get(1).equalsIgnoreCase("datetime") &&
                    referenceData.get(row).get(1).equalsIgnoreCase("datetime") &&
                    scannedData.get(row).get(column).equals("28.0") &&
                    referenceData.get(row).get(column).equals("29.0")
            ) {
                // this is a knopwn difference that will not show up in a dev environment, but it
                // does show up in Github actions
                return true;
            }
        }
        return false;
    }
    private static boolean isExcludedFromMatching(String tabName, int row, String scanValue, String referenceValue, DbType dbType) {
        if (tabName.equals("_")) {
            if (dbType == DELIMITED_TEXT_FILES) {
                switch (row) {
                    case 9:             // reference sheet does not contain DbType, ignore
                    //case 10:            // reference sheet contains 0
                        return true;
                }
            } else if (dbType != POSTGRESQL) {
                if (row == 9) {
                    return true;        // In reference sheet, this is always POSTGRESQL, ignore
                }
            }

            switch (row) {
                case 1:                // ignore WhiteRabbit version
                case 2: case 3:         // ignore timestamps
                    return true;
            }
        }

        return false;
    }
    private static boolean matchTypeName(String type, String reference, DbType dbType) {
        switch (dbType) {
            case ORACLE:
                switch (type) {
                    case "NUMBER": return reference.equals("integer");
                    case "VARCHAR2": return reference.equals("character varying");
                    case "FLOAT": return reference.equals("numeric");
                    // seems a mismatch in the OMOP CMD v5.2 (Oracle defaults to WITH time zone):
                    case "TIMESTAMP(6) WITH TIME ZONE": return reference.equals("timestamp without time zone");
                    default: throw new RuntimeException(String.format("Unsupported column type '%s' for DbType %s ", type, dbType.name()));
                }
            case SNOWFLAKE:
                switch (type) {
                    case "NUMBER": return reference.equals("integer") || reference.equals("numeric");
                    case "VARCHAR": return reference.equals("character varying");
                    case "TIMESTAMPNTZ": return reference.equals("timestamp without time zone");
                    default: throw new RuntimeException(String.format("Unsupported column type '%s' for DbType %s ", type, dbType.name()));
                }
            case MYSQL:
                switch (type) {
                    case "int":
                    case "decimal": return reference.equals("integer") || reference.equals("numeric");
                    case "varchar": return reference.equals("character varying");
                    case "timestamp": return reference.equals("timestamp without time zone");
                    default: throw new RuntimeException(String.format("Unsupported column type '%s' for DbType %s ", type, dbType.name()));
                }
            case SAS7BDAT:
                switch (type) {
                    case "VARCHAR": return reference.equals("INT");
                    default:
                        throw new RuntimeException(String.format("Unsupported column type '%s' for DbType %s ", type, dbType.name()));
                }
            case BIGQUERY:
                // Currently the bigquery verification on data types is rather permissive; this has to do with
                // how the test tables were created: as CSV uploads, leading to only a few data types.
                // As the main purpose of the bigquery tests is to verify that the JDBC diver is present
                // and works, this is considered not a (real) problem.
                switch (type) {
                    case "STRING": return reference.equals("character varying") || reference.equals("timestamp without time zone") || reference.equals("numeric") || reference.equals("integer");
                    case "INT64":
                    case "INTEGER": return reference.equals("integer") || reference.equals("numeric") || reference.equals("character varying");
                    default:
                        throw new RuntimeException(String.format("Unsupported column type '%s' for DbType %s ", type, dbType.name()));
                }
            default:
                throw new RuntimeException("Unsupported DbType: " + dbType.name());
        }
    }

    static class ColumnValueComparator implements Comparator<List<String>> {
        @Override
        public int compare(List<String> o1, List<String> o2) {
            if (o1.isEmpty() || o2.isEmpty()) {
                throw new RuntimeException("Nothing to compare...");
            }
            String firstString_o1 = o1.get(0);
            String firstString_o2 = o2.get(0);
            if (!firstString_o1.equalsIgnoreCase(firstString_o2) || (o1.size() == 1 || o2.size() == 1)) {
                // first field differs, or there is no second field to compare
                return firstString_o1.compareToIgnoreCase(firstString_o2);
            }
            // compare on the second field
            String secondString_o1 = o1.get(1);
            String secondString_o2 = o2.get(1);
            return secondString_o1.compareToIgnoreCase(secondString_o2);
        }
    }

    static private List<List<String>> transposeAndSort(List<List<String>> sheet) {
        List<List<String>> transposed = IntStream.range(0,sheet.get(0).size())
                .mapToObj(i -> sheet.stream().map(l -> l.get(i)).collect(Collectors.toList()))
                .collect(Collectors.toList());
        transposed.sort(new ColumnValueComparator());
        return transposed;
    }

    private static Map<String, List<List<String>>> readXlsxAsStringValues(Path xlsx) throws IOException {
        assertTrue(Files.exists(xlsx), String.format("File %s does not exist.", xlsx));

        Map<String, List<List<String>>> sheets = new HashMap<>();

        FileInputStream file = null;
        try {
            file = new FileInputStream(new File(xlsx.toUri()));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(String.format("File %s was expected to be found, but does not exist.", xlsx), e);
        }

        XSSFWorkbook xssfWorkbook = new XSSFWorkbook(file);

        for (int i = 0; i < xssfWorkbook.getNumberOfSheets(); ++i) {
            XSSFSheet xssfSheet = xssfWorkbook.getSheetAt(i);

            List<List<String>> sheet  = new ArrayList<>();
            for (org.apache.poi.ss.usermodel.Row row : xssfSheet) {
                List<String> values = new ArrayList<>();
                for (Cell cell: row) {
                    switch (cell.getCellType()) {
                        case NUMERIC: values.add(String.valueOf(cell.getNumericCellValue())); break;
                        case STRING: values.add(cell.getStringCellValue()); break;
                        default: throw new RuntimeException("Unsupported cell type: " + cell.getCellType().name());
                    };
                }
                sheet.add(values);
            }
            sheets.put(xssfSheet.getSheetName(), sheet);
        }

        return sheets;
    }

    public static class PropertiesFileChecker implements BooleanSupplier {
        private final String propertiesFileName;

        public PropertiesFileChecker(String propertiesFileName) {
            this.propertiesFileName = propertiesFileName;
        }

        @Override
        public boolean getAsBoolean() {
            String buildDirectory = System.getProperty("projectBuildDirectory");
            Path propertiesFilePath = Paths.get(buildDirectory,"../..", propertiesFileName);
            if (StringUtils.isNotEmpty(buildDirectory) && Files.exists(propertiesFilePath)) {
                try {
                    loadSystemProperties(propertiesFilePath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
            // check the endpoint here and return either true or false
            return false;
        }

        private void loadSystemProperties(Path envVarFile) throws IOException {
            Files.lines(envVarFile)
                    .map(line -> line.replaceAll("^export ", ""))
                    .map(line2 -> line2.split("=", 2))
                    .forEach(v -> System.setProperty(v[0], v[1]));
        }
    }

    @FunctionalInterface
    public interface ReaderInterface {
        String getOrFail(String name);
    }

    public static class EnvironmentReader implements ScanTestUtils.ReaderInterface {
        public String getOrFail(String name) {
            return getEnvOrFail(name);
        }
    }
    public static class PropertyReader implements ScanTestUtils.ReaderInterface {
        public String getOrFail(String name) {
            return getPropertyOrFail(name);
        }
    }

    public static String getEnvOrFail(String name) {
        String value = System.getenv(name);
        if (StringUtils.isEmpty(value)) {
            throw new RuntimeException(String.format("Environment variable '%s' is not set.", name));
        }

        return value;
    }

    public static String getPropertyOrFail(String name) {
        String value = System.getProperty(name);
        if (StringUtils.isEmpty(value)) {
            throw new RuntimeException(String.format("System property '%s' is not set.", name));
        }

        return value;
    }
}
