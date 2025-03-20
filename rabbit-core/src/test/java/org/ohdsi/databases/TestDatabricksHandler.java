package org.ohdsi.databases;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
}
