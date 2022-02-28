package com.arcadia.whiteRabbitService.service.task;

import java.util.Optional;
import java.util.concurrent.Future;

/*
* P - task parameter
* R - task result
* */
public interface TaskHandler<P, R> {

    boolean createTask(P param, String id);

    Optional<R> getTaskResult(String id);

    void cancelTask(String id);
}
