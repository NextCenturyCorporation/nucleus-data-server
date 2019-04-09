package com.ncc.neon.server.controllers;


import com.ncc.neon.server.models.datasource.DataSet;
import com.ncc.neon.server.services.DataSetService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("dataset")
@Slf4j
public class DataSetController {
    private DataSetService dataSetService;

    DataSetController(DataSetService dataSetService) {
        this.dataSetService = dataSetService;
    }

    @GetMapping(path = "all", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Flux<DataSet> getAll() {
        return Flux.fromIterable(dataSetService.getDataSets());
    }

}
