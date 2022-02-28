package org.ohdsi.whiteRabbit;

public class ThreadInterrupter implements Interrupter {
    @Override
    public void checkWasInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Process was canceled by User");
        }
    }
}
