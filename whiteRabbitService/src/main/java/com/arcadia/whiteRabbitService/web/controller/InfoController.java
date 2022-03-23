package com.arcadia.whiteRabbitService.web.controller;

import com.arcadia.whiteRabbitService.service.response.InfoResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.ResponseEntity.ok;

@RestController
@RequestMapping("/api/info")
@Slf4j
public class InfoController {
    @GetMapping()
    public ResponseEntity<InfoResponse> getInfo() {
        log.info("Rest request to get App info");
        return ok(new InfoResponse(0.4, "White Rabbit"));
    }
}
