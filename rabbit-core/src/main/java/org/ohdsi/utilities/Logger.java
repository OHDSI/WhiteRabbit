package org.ohdsi.utilities;

public interface Logger {
    void log(String message);

    void logWithTime(String message);

    void error(String message);

    void cancel(String message);

    void failed(String message);
}
