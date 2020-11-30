package com.arcadia.whiteRabbitService.service;

import lombok.SneakyThrows;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

@Service
public class ScanTasksHandler implements TaskHandler<byte[]> {

    private final Map<String, Future<byte[]>> scanTasks = new HashMap<>();

    @SneakyThrows
    @Override
    public byte[] handleTask(String id, Future<byte[]> task) {
        addTask(id, task);
        var result = task.get();
        finishTask(id);

        return result;
    }

    @Override
    public void addTask(String id, Future<byte[]> task) {
        this.scanTasks.put(id, task);
    }

    @Override
    public void finishTask(String id) {
        scanTasks.remove(id);
    }

    @Override
    public void cancelTask(String id) {
        final Future<byte[]> future = scanTasks.remove(id);

        if (future != null) {
            future.cancel(true);
        }
    }
}
