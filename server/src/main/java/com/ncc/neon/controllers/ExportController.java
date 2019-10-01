package com.ncc.neon.controllers;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.ExportQuery;
import com.ncc.neon.models.queries.FieldNamePrettyNamePair;
import com.ncc.neon.models.results.ExportResult;
import com.ncc.neon.models.results.TabularQueryResult;
import com.ncc.neon.services.QueryService;

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
import java.util.stream.Collectors;

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

    private String GetCSVHeader(List<FieldNamePrettyNamePair> fieldNamePrettyNamePairs)
    {
        return String.join(Comma, fieldNamePrettyNamePairs.stream().map(pair -> pair.getPretty()).collect(Collectors.toList()));
    }

    private String GetCSVRecord(Map<String, Object> map, List<FieldNamePrettyNamePair> fieldNamePrettyNamePairs)
    {
        StringBuilder sb = new StringBuilder();
        List<String> fieldNames = fieldNamePrettyNamePairs.stream().map(pair -> pair.getQuery()).collect(Collectors.toList());
        for(int index = 0; index < fieldNames.size(); index++)
        {
            String fieldName = fieldNames.get(index);
            if (map.containsKey(fieldName))
            {
                Object value = map.get(fieldName);
                if (value instanceof String)
                {//put string in double quotes in case field separator comma is part of the string
                    sb.append(String.format("\"%s\"", value));
                }
                else
                {
                    sb.append(value);
                }
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
     * @return The result of the export containing the export file name and the export data in csv formt
     */
    @PostMapping(path = "csv", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public Mono<ResponseEntity<ExportResult>> exportToCSV(@RequestBody ExportQuery exportQuery) throws IOException
    {
        log.debug("Export parameters: " + exportQuery.toString());

        if (exportQuery.getFieldNamePrettyNamePairs().isEmpty() || exportQuery.getDataStoreType().isBlank() || exportQuery.getHostName().isBlank())
        {
            return Mono.just(ResponseEntity.badRequest().body(new ExportResult(exportQuery.getFileName(), "missing hostName, dataStoreType or queryFieldNameMap")));
        }

        ConnectionInfo ci = new ConnectionInfo(exportQuery.getDataStoreType(), exportQuery.getHostName());
        Mono<TabularQueryResult> monoResult = queryService.executeQuery(ci, exportQuery.getQuery());

        List<String> csvRecords = new ArrayList<String>();  
        StringBuilder csvFileContent = new StringBuilder();

        //add header record
        String csvHeader = GetCSVHeader(exportQuery.getFieldNamePrettyNamePairs());
        csvRecords.add(csvHeader);

        //add data records
        monoResult
        .map(result -> {
            List<Map<String, Object>> list = result.getData();
            
            list.forEach(map -> csvRecords.add(GetCSVRecord(map, exportQuery.getFieldNamePrettyNamePairs())));
            return csvRecords;
        })
        .subscribe(csvContent -> {
            csvFileContent.append(String.join(System.lineSeparator(), csvRecords));
        });

        ExportResult exportResult = new ExportResult(exportQuery.getFileName(), csvFileContent.toString());
        return Mono.just(ResponseEntity.ok()
        .body(exportResult));       

    }    
}

