package com.arcadia.whiteRabbitService.service;

import com.arcadia.whiteRabbitService.service.request.FileSaveRequest;
import com.arcadia.whiteRabbitService.service.response.FileSaveResponse;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.filter.FormContentFilter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = RANDOM_PORT)
class FilesManagerServiceTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    WRTestRestTemplate testRestTemplate;

    FilesManagerService filesManagerService;

    @Autowired
    FormContentFilter orderedFormContentFilter;

    @Autowired
    MappingJackson2HttpMessageConverter jacksonMessageConverter;

    @BeforeEach
    void setUp() {
        filesManagerService = new FilesManagerServiceImpl(testRestTemplate);
    }

//    @Test
//    void saveAndGetFile() {
//        String fileName = "cprd_1k.etl";
//        byte[] bytes = readFileFromResourcesAsByteArray(getClass(), fileName);
//        MockMultipartFile multipartFile = new MockMultipartFile(fileName, bytes);
//        FileSaveRequest saveRequest = new FileSaveRequest(
//                "Test WhiteRabbit",
//                "white-rabbit",
//                multipartFile
//        );
//        FileSaveResponse saveResponse = filesManagerService.saveFile(saveRequest);
//
//        assertNotNull(saveResponse);
//        assertNotNull(saveResponse.getHash());
//        assertEquals(saveRequest.getUsername(), saveResponse.getUsername());
//        assertEquals(saveRequest.getDataKey(), saveResponse.getDataKey());
//
//        ByteArrayResource resource = filesManagerService.getFile(saveResponse.getHash());
//        byte[] resultBytes = resource.getByteArray();
//
//        assertEquals(bytes.length, resultBytes.length);
//        assertArrayEquals(bytes, resultBytes);
//    }

    @SneakyThrows
    static byte[] readFileFromResourcesAsByteArray(Class<?> currentClass, String fileName) {
        InputStream inputStream = Optional.ofNullable(currentClass.getClassLoader().getResourceAsStream(fileName))
                .orElseThrow(() -> new RuntimeException("Can not open file " + fileName));
        return inputStream.readAllBytes();
    }

    @SneakyThrows
    static File readFileFromResources(Class<?> currentClass, String fileName) {
        URL resource = Optional.ofNullable(currentClass.getClassLoader().getResource(fileName))
                .orElseThrow(() -> new RuntimeException("Can not open file " + fileName));
        return new File(resource.toURI());
    }
}