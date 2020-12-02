package com.arcadia.whiteRabbitService.service;

import org.springframework.lang.NonNull;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.WebSocketHandlerDecorator;

public class WhiteRabbitWebSocketHandlerDecorator extends WebSocketHandlerDecorator {

    private final ScanTasksHandler scanTasksHandler;

    public WhiteRabbitWebSocketHandlerDecorator(WebSocketHandler delegate, ScanTasksHandler scanTasksHandler) {
        super(delegate);
        this.scanTasksHandler = scanTasksHandler;
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, @NonNull CloseStatus closeStatus) throws Exception {
        scanTasksHandler.cancelTask(session.getId());
        super.afterConnectionClosed(session, closeStatus);
    }

    @Override
    public boolean supportsPartialMessages() {
        return true;
    }
}
