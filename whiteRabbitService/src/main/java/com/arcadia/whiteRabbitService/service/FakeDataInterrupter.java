package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataConversion;
import com.arcadia.whiteRabbitService.repository.FakeDataConversionRepository;
import lombok.RequiredArgsConstructor;
import org.ohdsi.whiteRabbit.Interrupter;
import org.springframework.web.server.ResponseStatusException;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.ABORTED;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RequiredArgsConstructor
public class FakeDataInterrupter implements Interrupter {
    private final FakeDataConversionRepository conversionRepository;
    private final Long conversionId;

    @Override
    public void checkWasInterrupted() throws InterruptedException {
        FakeDataConversion conversion = conversionRepository.findById(conversionId)
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Conversion not found by id " + conversionId));
        if (conversion.getStatusCode() == ABORTED.getCode()) {
            throw new InterruptedException("Scanning process was canceled by User");
        }
    }
}
