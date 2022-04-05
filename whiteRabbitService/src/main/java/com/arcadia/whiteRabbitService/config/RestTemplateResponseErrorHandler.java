package com.arcadia.whiteRabbitService.config;

import com.arcadia.whiteRabbitService.service.error.NotFoundException;
import com.arcadia.whiteRabbitService.service.error.ServerErrorException;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatus.Series;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;

import java.io.IOException;

import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.Series.CLIENT_ERROR;
import static org.springframework.http.HttpStatus.Series.SERVER_ERROR;

public class RestTemplateResponseErrorHandler implements ResponseErrorHandler {
    @Override
    public boolean hasError(ClientHttpResponse response) throws IOException {
        Series series = response.getStatusCode().series();
        return series == CLIENT_ERROR || series == SERVER_ERROR;
    }

    @Override
    public void handleError(ClientHttpResponse response) throws IOException {
        if (response.getStatusCode().series() == SERVER_ERROR) {
            throw new ServerErrorException(response.getBody().toString());
        } else if (response.getStatusCode().series() == HttpStatus.Series.CLIENT_ERROR) {
            if (response.getStatusCode() == NOT_FOUND) {
                throw new NotFoundException(response.getBody().toString());
            }
        }
        throw new RuntimeException(response.getBody().toString());
    }
}
