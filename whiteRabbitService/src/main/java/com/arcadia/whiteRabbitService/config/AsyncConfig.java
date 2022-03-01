package com.arcadia.whiteRabbitService.config;

import org.springframework.boot.task.TaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AsyncConfig {
    @Bean
    public Executor asyncExecutor() {
        ThreadPoolTaskExecutor executor = new TaskExecutorBuilder()
                .corePoolSize(16)
                .maxPoolSize(16)
                .queueCapacity(16)
                .threadNamePrefix("WhiteRabbit-")
                .build();
        executor.initialize();

        return executor;
    }
}
