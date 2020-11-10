package org.ohdsi.utilities;

public class ConsoleLogger implements Logger {
    @Override
    public void log(String message) {
        System.out.println(message);
    }

    @Override
    public void logWithTime(String message) {
        StringUtilities.outputWithTime(message);
    }

    @Override
    public void error(String message) {
        System.out.println(message);
    }
}
