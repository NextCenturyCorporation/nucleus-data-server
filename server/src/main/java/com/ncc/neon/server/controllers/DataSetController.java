package com.ncc.neon.server.controllers;


import com.ncc.neon.server.models.datasource.*;
import com.ncc.neon.server.services.DataConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("dataset")
@Slf4j
public class DataSetController {
    private DataConfigService dataSetService;

    DataSetController(DataConfigService dataSetService) {
        this.dataSetService = dataSetService;
    }

    @GetMapping(path = "all", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<DataConfig> getAll() {
        return Mono.just(dataSetService.getDataConfig());
    }


    @GetMapping(path="databasenames/{dataSetName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<List<String>> getDatabaseNames(@PathVariable String dataSetName) {
        return Mono.just(dataSetService.getDataConfig().getDatabaseNames(dataSetName));
    }

    @GetMapping(path="tablenames/{dataSetName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<List<String>> getTableNames(@PathVariable String dataSetName) {
        return Mono.just(dataSetService.getDataConfig().getDatabaseNames(dataSetName));
    }

    @GetMapping(path="tablesandfields/{dataSetName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<List<String>> getTablesAndFields(@PathVariable String dataSetName) {
        List<String> tablesAndFields = new ArrayList<>();

        List<Table> tables = dataSetService.getDataConfig().getTables(dataSetName);
        for (Table table : tables) {
            for (Field field : table.getFields()) {
                tablesAndFields.add(field.toString());
            }
        }

        return Mono.just(tablesAndFields);
    }

    @GetMapping(path="fields/types/{dataSetName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<Map<String, String>> getFieldsAndTypes(@PathVariable String dataSetName) {
        Map<String, String> fieldsAndTypes = new HashMap<>();

        List<Table> tables = dataSetService.getDataConfig().getTables(dataSetName);
        for (Table table : tables) {
            for (Field field : table.getFields()) {
                fieldsAndTypes.put(field.toString(), field.getType());
            }
        }

        return Mono.just(fieldsAndTypes);
    }

}
