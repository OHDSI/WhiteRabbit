package org.ohdsi.utilities;

public interface Logger {
    void info(String message);

    void debug(String message);

    void warning(String message);

    void error(String message);
}
