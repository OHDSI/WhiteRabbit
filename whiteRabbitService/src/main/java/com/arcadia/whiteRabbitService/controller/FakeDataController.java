package com.arcadia.whiteRabbitService.controller;

import com.arcadia.whiteRabbitService.dto.ParamsForFakeDataGenerationDto;
import com.arcadia.whiteRabbitService.service.WhiteRabbitFacade;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/fake-data")
@AllArgsConstructor
public class FakeDataController {

    private final WhiteRabbitFacade whiteRabbitFacade;

    @PostMapping
    public String generate(@RequestBody ParamsForFakeDataGenerationDto dto) {
        try {
            whiteRabbitFacade.generateFakeData(dto);
            return "Fake data successfully generated";
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e.getCause());
        }
    }
}
