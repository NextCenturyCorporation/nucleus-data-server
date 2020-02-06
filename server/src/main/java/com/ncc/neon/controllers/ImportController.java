package com.ncc.neon.controllers;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.ImportQuery;
import com.ncc.neon.models.results.ActionResult;
import com.ncc.neon.services.QueryService;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("importservice")
@Slf4j
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
    @PostMapping(path="/", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Mono<ActionResult>> importData(@RequestBody ImportQuery importQuery)
    {
        log.debug(
            String.format("Export parameters: storeType: %s, host: %s, db: %s, table: %s, recordCount: %d ", 
            importQuery.getDataStoreType(),
            importQuery.getHostName(),
            importQuery.getDatabase(),
            importQuery.getTable(),
            importQuery.getSource() == null ? null : importQuery.getSource().size()
        ));

        if (StringUtils.isBlank(importQuery.getDataStoreType()) ||
            StringUtils.isBlank(importQuery.getHostName()) ||
            StringUtils.isBlank(importQuery.getDatabase()) ||
            StringUtils.isBlank(importQuery.getTable()) ||
            CollectionUtils.isEmpty(importQuery.getSource())
        ) {
            String error = "one or more missing parameters (dataStoreType, hostName, database, table, source).";
            return ResponseEntity.badRequest().body(Mono.just(new ActionResult(error)));
        }

        ConnectionInfo ci = new ConnectionInfo(importQuery.getDataStoreType(), importQuery.getHostName());

        Mono<ActionResult> response = Mono.create(sink -> {
            queryService.getDatabaseNames(ci).collectList().subscribe(databases -> {
                if (databases.contains(importQuery.getDatabase()) && importQuery.isNew()) {
                    sink.success(new ActionResult(String.format("A database with the same name '%s' already exists.", importQuery.getDatabase())));
                }
                else if (!databases.contains(importQuery.getDatabase()) && !importQuery.isNew()) {
                    sink.success(new ActionResult(String.format("Database '%s' does not exist.", importQuery.getDatabase())));
                }
                else {
                    queryService.importData(ci, importQuery).subscribe(result -> {
                        sink.success(result);
                    });
                }
            });          
        });
        
        return ResponseEntity.ok().body(response);    
    }    
}
