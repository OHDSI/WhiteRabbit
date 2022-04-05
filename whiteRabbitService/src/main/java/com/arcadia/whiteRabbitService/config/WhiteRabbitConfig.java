package com.arcadia.whiteRabbitService.config;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
@RequiredArgsConstructor
public class WhiteRabbitConfig {
    private final RestTemplateBuilder restTemplateBuilder;

    @Bean
    public RestTemplate restTemplate() {
        return restTemplateBuilder
                .errorHandler(new RestTemplateResponseErrorHandler())
                .build();
    }
}
