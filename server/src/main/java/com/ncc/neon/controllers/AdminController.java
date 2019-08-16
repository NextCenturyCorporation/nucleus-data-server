package com.ncc.neon.controllers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.http.ResponseEntity;
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
    Map<String, Object> administrationProperties = new LinkedHashMap<String, Object>();

    AdminController() {
        try {
            File infoFile = new File(ClassLoader.getSystemClassLoader().getResource("autogenerate.properties").getFile());
            if(infoFile.exists()) {
                Arrays.stream(new String(Files.readAllBytes(infoFile.toPath())).split("\n")).forEach(line -> {
                    String[] data = line.split(" = ");
                    administrationProperties.put(data[0], data[1]);
                });
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (NullPointerException e) {
            e.printStackTrace();
        }
        log.debug("Administration Properties: " + administrationProperties.toString());
    }

    @GetMapping(path = "status")
    public ResponseEntity<Mono<Map<String, Object>>> status() {
        Map<String, Object> status = new LinkedHashMap<String, Object>(administrationProperties);
        return ResponseEntity.ok().body(Mono.just(status));
    }
}
