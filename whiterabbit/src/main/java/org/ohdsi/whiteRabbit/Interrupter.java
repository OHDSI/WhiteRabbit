package org.ohdsi.whiteRabbit;

public interface Interrupter {
    void checkWasInterrupted() throws InterruptedException;
}
