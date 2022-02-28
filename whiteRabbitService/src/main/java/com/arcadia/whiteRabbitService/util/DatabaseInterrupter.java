package com.arcadia.whiteRabbitService.util;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import lombok.RequiredArgsConstructor;
import org.ohdsi.whiteRabbit.Interrupter;
import org.springframework.web.server.ResponseStatusException;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.ABORTED;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RequiredArgsConstructor
public class DatabaseInterrupter implements Interrupter {
    private final ScanDataConversionRepository scanDataConversionRepository;
    private final Long conversionId;

    @Override
    public void checkWasInterrupted() throws InterruptedException {
        ScanDataConversion conversion = scanDataConversionRepository.findById(conversionId)
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Conversion not found by id " + conversionId));
        if (conversion.getStatusCode() == ABORTED.getCode()) {
            throw new InterruptedException("Scanning process was canceled by User");
        }
    }
}
