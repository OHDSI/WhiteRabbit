package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.model.scandata.ScanDataConversion;
import com.arcadia.whiteRabbitService.repository.ScanDataConversionRepository;
import com.arcadia.whiteRabbitService.repository.ScanDataResultRepository;
import com.arcadia.whiteRabbitService.service.response.FileSaveResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;

import static com.arcadia.whiteRabbitService.model.ConversionStatus.*;
import static com.arcadia.whiteRabbitService.service.FilesManagerServiceTest.readFileFromResources;
import static com.arcadia.whiteRabbitService.service.ScanDataConversionServiceTest.createScanDataConversion;
import static com.arcadia.whiteRabbitService.service.ScanDataResultServiceImpl.DATA_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@ExtendWith(SpringExtension.class)
class ScanDataResultServiceTest {
    @Autowired
    ScanDataConversionRepository conversionRepository;

    @Autowired
    ScanDataResultRepository resultRepository;

    @MockBean
    FilesManagerService filesManagerService;

    ScanDataResultService resultService;

    @BeforeEach
    void setUp() {
        resultService = new ScanDataResultServiceImpl(
                conversionRepository,
                resultRepository,
                filesManagerService
        );
    }

    @Test
    void saveCompletedResult() {
        String fileName = "mdcd_native_test.xlsx";
        File scanReportFile = readFileFromResources(getClass(), fileName);
        String fileHash = "test-hash";
        ScanDataConversion conversion = createScanDataConversion();
        conversionRepository.save(conversion);
        Mockito.when(filesManagerService.saveFile(Mockito.any()))
                .thenReturn(new FileSaveResponse(fileHash, conversion.getUsername(), DATA_KEY));
        resultService.saveCompletedResult(scanReportFile, conversion.getId());
        conversion = conversionRepository.findById(conversion.getId()).get();

        assertNotNull(conversion.getResult());
        assertEquals(COMPLETED.getCode(), conversion.getStatusCode());
        assertEquals(COMPLETED.getName(), conversion.getStatusName());
        assertEquals(fileHash, conversion.getResult().getFileKey());
    }

    @Test
    void saveFailedResult() {
        ScanDataConversion conversion = createScanDataConversion();
        conversionRepository.save(conversion);
        resultService.saveFailedResult(conversion.getId());
        conversion = conversionRepository.findById(conversion.getId()).get();

        assertEquals(FAILED.getCode(), conversion.getStatusCode());
        assertEquals(FAILED.getName(), conversion.getStatusName());
    }

    @Test
    void saveAbortedResult() {
        ScanDataConversion conversion = createScanDataConversion();
        conversionRepository.save(conversion);
        resultService.saveAbortedResult(conversion.getId());
        conversion = conversionRepository.findById(conversion.getId()).get();

        assertEquals(ABORTED.getCode(), conversion.getStatusCode());
        assertEquals(ABORTED.getName(), conversion.getStatusName());
    }
}