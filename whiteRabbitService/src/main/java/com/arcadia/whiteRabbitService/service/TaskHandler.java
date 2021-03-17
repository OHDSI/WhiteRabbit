package com.arcadia.whiteRabbitService.service;

import java.util.concurrent.Future;

public interface TaskHandler<T> {

    T handleTask(String id, Future<T> task);

    void addTask(String id, Future<T> task);

    void finishTask(String id);

    void cancelTask(String id);
}
