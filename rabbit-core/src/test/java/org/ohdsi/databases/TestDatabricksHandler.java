package org.ohdsi.databases;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TestDatabricksHandler {
    Logger logger = LoggerFactory.getLogger(TestDatabricksHandler.class);

    @Test
    public void testGetInstance() {
        StorageHandler instance = DatabricksHandler.INSTANCE.getInstance(null);
    }
}
