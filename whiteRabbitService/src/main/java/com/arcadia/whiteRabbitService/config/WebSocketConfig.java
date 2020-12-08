package com.arcadia.whiteRabbitService.config;

import com.arcadia.whiteRabbitService.service.FakeTasksHandler;
import com.arcadia.whiteRabbitService.service.ScanTasksHandler;
import com.arcadia.whiteRabbitService.service.WhiteRabbitWebSocketHandlerDecorator;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;
import org.springframework.web.socket.handler.WebSocketHandlerDecorator;

@AllArgsConstructor
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final ScanTasksHandler scanTasksHandler;

    private final FakeTasksHandler fakeTasksHandler;

    @Bean
    @NonNull
    public WebSocketHandlerDecorator webSocketHandlerDecorator(WebSocketHandler webSocketHandler) {
        return new WhiteRabbitWebSocketHandlerDecorator(webSocketHandler, scanTasksHandler, fakeTasksHandler);
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        config.enableSimpleBroker("/queue");
        config.setApplicationDestinationPrefixes("/white-rabbit-service");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry
                .addEndpoint("/scan-report/db", "/scan-report/file", "/fake-data")
                .setAllowedOrigins("*")
                .withSockJS();
    }

    @Override
    public void configureWebSocketTransport(WebSocketTransportRegistration registry) {
        registry
                .addDecoratorFactory(this::webSocketHandlerDecorator)
                .setMessageSizeLimit(1024 * 1024 * 10); // 10Mb
    }
}
