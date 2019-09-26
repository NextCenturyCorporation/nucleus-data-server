package com.ncc.neon.controllers;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.ExportQuery;
import com.ncc.neon.models.results.TabularQueryResult;
import com.ncc.neon.services.QueryService;

import org.elasticsearch.ResourceNotFoundException;
import org.springframework.core.Constants;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("exportservice")
@Slf4j

public class ExportController {
    private final String Comma = ",";
    private QueryService queryService;

    ExportController(QueryService queryService) {
        this.queryService = queryService;
    }

    private String GetCSVHeader(Map<String, String> fieldNameMap)
    {
        return String.join(Comma, fieldNameMap.values());
    }

    private String GetCSVRecord(Map<String, Object> map, Map<String, String> fieldNameMap)
    {
        StringBuilder sb = new StringBuilder();
        List<String> fieldNames = new ArrayList<>(fieldNameMap.keySet());
        for(int index = 0; index < fieldNames.size(); index++)
        {
            String fieldName = fieldNames.get(index);
            if (map.containsKey(fieldName))
            {
                sb.append(map.get(fieldName));
            }

            if (index < fieldNames.size() - 1)
            {
                sb.append(Comma);
            }
        }

        return sb.toString();
    }

    /**
     * Executes a query against the supplied connection and returns the result in CSV format
     * 
     * @param exportQuery  export parameters
     */
    @PostMapping(path = "csv", produces = MediaType.TEXT_PLAIN_VALUE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public Mono<ResponseEntity<String>> exportToCSV(@RequestBody ExportQuery exportQuery) throws IOException
    {
        log.debug("Export parameters: " + exportQuery.toString());

        if (exportQuery.getQueryFieldNameMap().isEmpty() || exportQuery.getDatabaseType().isBlank() || exportQuery.getHostName().isBlank())
        {
            return Mono.just(ResponseEntity.badRequest().body("Missing hostName, databaseType or queryFieldNameMap"));
        }

        ConnectionInfo ci = new ConnectionInfo(exportQuery.getDatabaseType(), exportQuery.getHostName());
        Mono<TabularQueryResult> monoResult = queryService.executeQuery(ci, exportQuery.getQuery());

        List<String> csvRecords = new ArrayList<String>();  
        StringBuilder csvFileContent = new StringBuilder();

        //add header record
        String csvHeader = GetCSVHeader(exportQuery.getQueryFieldNameMap());
        csvRecords.add(csvHeader);

        //add data records
        monoResult
        .map(result -> {
            List<Map<String, Object>> list = result.getData();
            
            list.forEach(map -> {
                String csvRecord = GetCSVRecord(map, exportQuery.getQueryFieldNameMap());
                if (!csvRecord.isBlank())
                {
                    csvRecords.add(csvRecord);
                }                
            });

            return csvRecords;
        })
        .subscribe(csvContent -> {
            csvFileContent.append(String.join(System.lineSeparator(), csvRecords));
        });

        return Mono.just(ResponseEntity.ok()
        .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN_VALUE)
        .header(HttpHeaders.CONTENT_DISPOSITION, String.format( "attachment; filename=\"%s\"", exportQuery.getFileName()))
        .body(csvFileContent.toString()));       

    }    
}

