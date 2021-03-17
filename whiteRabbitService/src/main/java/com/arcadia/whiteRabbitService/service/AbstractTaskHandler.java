package com.arcadia.whiteRabbitService.service;

import lombok.SneakyThrows;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public abstract class AbstractTaskHandler<T> implements TaskHandler<T> {

    protected Map<String, Future<T>> tasks = new ConcurrentHashMap<>();

    @SneakyThrows
    @Override
    public T handleTask(String id, Future<T> task) {
        addTask(id, task);
        T result = task.get();
        finishTask(id);

        return result;
    }

    @Override
    public void addTask(String id, Future<T> task) {
        this.tasks.put(id, task);
    }

    @Override
    public void finishTask(String id) {
        tasks.remove(id);
    }

    @Override
    public void cancelTask(String id) {
        final Future<T> future = tasks.remove(id);

        if (future != null) {
            future.cancel(true);
        }
    }
}
