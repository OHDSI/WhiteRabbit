package org.ohdsi.databases;

import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Test;
import org.ohdsi.databases.configuration.ConfigurationField;
import org.ohdsi.databases.configuration.ScanConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.ohdsi.databases.DatabricksHandler.getRowSampleQueryStaticForResolvedTableName;

public class TestDatabricksHandler {
    Logger logger = LoggerFactory.getLogger(TestDatabricksHandler.class);

    @Test
    public void testSamplePercentageWithinRange() {
        // extreme cases should return 1% and 100% respectively
        assertTrue(getRowSampleQueryStaticForResolvedTableName("table", 100000, 1)
                .contains("(1 PERCENT"));
        assertTrue(getRowSampleQueryStaticForResolvedTableName("table", 1, 100000)
                .contains("(100 PERCENT"));

        // test a few "normal" values
        assertTrue(getRowSampleQueryStaticForResolvedTableName("table", 1000, 100)
                .contains("(10 PERCENT"));
        assertTrue(getRowSampleQueryStaticForResolvedTableName("table", 1000, 900)
                .contains("(90 PERCENT"));
    }

    @Test
    void testPrintIniFileTemplate() throws IOException {
        String output;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); PrintStream printStream = new PrintStream(outputStream)) {
            ScanConfiguration configuration = new DatabricksHandler.DatabricksConfiguration();
            configuration.printIniFileTemplate(printStream);
            output = outputStream.toString();
            for (ConfigurationField field: configuration.getFields()) {
                assertTrue(output.contains(field.name), String.format("ini file template should contain field name (%s)", field.name));
                assertTrue(output.contains(field.toolTip), String.format("ini file template should contain tool tip (%s)", field.toolTip));
                if (!StringUtils.isEmpty(field.getDefaultValue())) {
                    assertTrue(output.contains(field.getDefaultValue()), String.format("ini file template should contain default value (%s)", field.getDefaultValue()));
                }
            }
        }
    }
}
