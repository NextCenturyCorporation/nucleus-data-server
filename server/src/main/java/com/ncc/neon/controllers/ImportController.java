package com.ncc.neon.controllers;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.ImportQuery;
import com.ncc.neon.models.results.ImportResult;
import com.ncc.neon.services.QueryService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("importservice")
public class ImportController {
    private QueryService queryService;

    ImportController(QueryService queryService) {
        this.queryService = queryService;
    }

    /**
     * imports JSON formated data to a data store
     * 
     * @param importQuery object containing import parameters
     * @return import result containing total and failed counts
     */
    @PostMapping(path="/", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Flux<ImportResult>> importData(@RequestBody ImportQuery importQuery)
    {
        ConnectionInfo ci = new ConnectionInfo(importQuery.getDataStoreType(), importQuery.getHostName());

        Flux<ImportResult> response = queryService.addData(ci, importQuery.getDatabase(), importQuery.getTable(), importQuery.getSource());

        return ResponseEntity.ok().body(response);
    }    
}
