package org.ohdsi.whiteRabbit;

public interface CanInterrupt {

    default void checkWasInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Process was canceled by User");
        }
    }
}
