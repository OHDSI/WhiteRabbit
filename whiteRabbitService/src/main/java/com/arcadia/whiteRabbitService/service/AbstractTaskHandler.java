package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.service.error.FailedToScanException;
import lombok.SneakyThrows;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import static java.util.Optional.empty;
import static java.util.Optional.of;

public abstract class AbstractTaskHandler<P, R> implements TaskHandler<P, R> {

    protected Map<String, Future<R>> tasks = new ConcurrentHashMap<>();

    @SneakyThrows
    @Override
    public boolean createTask(P param, String id) {
        if (tasks.containsKey(id)) {
            return false;
        }
        Future<R> task = task(param, id);
        tasks.put(id, task);
        return true;
    }

    @SneakyThrows
    public Optional<R> getTaskResult(String id) {
        Future<R> task = tasks.remove(id);
        return task != null ? of(task.get()) : empty();
    }

    @Override
    public void cancelTask(String id) {
        final Future<R> future = tasks.remove(id);
        if (future != null) {
            future.cancel(true);
        }
    }

    protected abstract Future<R> task(P param, String id) throws Exception;
}
