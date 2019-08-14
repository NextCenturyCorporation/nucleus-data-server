package com.ncc.neon.controllers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

import reactor.core.publisher.Mono;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("admin")
@Slf4j
public class AdminController {

    String gitCommit = "HEAD";

    AdminController() {
        try {
            File infoFile = ResourceUtils.getFile("../custom.properties");
            if(infoFile.exists()) {
                gitCommit = new String(Files.readAllBytes(infoFile.toPath()));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @GetMapping(path = "status")
    public ResponseEntity<Mono<Map<String, Object>>> status() {
        Map<String, Object> status = Map.ofEntries(
            Map.entry("Git Commit", gitCommit)
        );
        return ResponseEntity.ok().body(Mono.just(status));
    }
}
