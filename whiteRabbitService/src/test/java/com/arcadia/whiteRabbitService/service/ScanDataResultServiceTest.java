package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataResultRepository;
import com.arcadia.whiteRabbitService.service.response.FileSaveResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.*;
import static com.arcadia.whiteRabbitService.service.FilesManagerServiceTest.readFileFromResources;
import static com.arcadia.whiteRabbitService.service.ScanDataConversionServiceTest.createConversion;
import static com.arcadia.whiteRabbitService.service.ScanDataResultServiceImpl.DATA_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
class ScanDataResultServiceTest {
    @MockBean
    ScanDataResultRepository resultRepository;

    @MockBean
    ScanDataConversionRepository conversionRepository;

    @MockBean
    FilesManagerService filesManagerService;

    ScanDataResultService resultService;

    @BeforeEach
    void setUp() {
        resultService = new ScanDataResultServiceImpl(
                resultRepository,
                conversionRepository,
                filesManagerService
        );
    }

    @Test
    void saveCompletedResult() {
        String fileName = "mdcd_native_test.xlsx";
        File scanReportFile = readFileFromResources(getClass(), fileName);
        ScanDataConversion conversion = createConversion();
        String fileHash = "test-hash";

        Mockito.when(filesManagerService.saveFile(Mockito.any()))
                .thenReturn(new FileSaveResponse(fileHash, conversion.getUsername(), DATA_KEY));

        resultService.saveCompletedResult(scanReportFile, conversion);

        assertNotNull(conversion.getResult());
        assertEquals(conversion.getStatusCode(), COMPLETED.getCode());
        assertEquals(conversion.getStatusName(), COMPLETED.getName());
        assertEquals(fileHash, conversion.getResult().getFileKey());
    }

    @Test
    void saveFailedResult() {
        ScanDataConversion conversion = createConversion();
        String errorMessage = "Test error";

        resultService.saveFailedResult(conversion, errorMessage);

        assertEquals(conversion.getStatusCode(), FAILED.getCode());
        assertEquals(conversion.getStatusName(), FAILED.getName());
    }

    @Test
    void saveAbortedResult() {
        ScanDataConversion conversion = createConversion();

        resultService.saveAbortedResult(conversion);

        assertEquals(conversion.getStatusCode(), ABORTED.getCode());
        assertEquals(conversion.getStatusName(), ABORTED.getName());
    }
}