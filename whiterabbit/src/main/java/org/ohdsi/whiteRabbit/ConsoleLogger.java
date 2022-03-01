package org.ohdsi.whiteRabbit;

import org.ohdsi.utilities.StringUtilities;

public class ConsoleLogger implements Logger {
    @Override
    public void info(String message) {
        StringUtilities.outputWithTime(message);
    }

    @Override
    public void debug(String message) {
        info("DEBUG: " + message);
    }

    @Override
    public void warning(String message) {
        info("WARNING: " + message);
    }

    @Override
    public void error(String message) {
        System.err.println(message);
    }

    @Override
    public void setItemsCount(int itemsCount) {
    }

    @Override
    public void incrementScannedItems() {
    }
}
