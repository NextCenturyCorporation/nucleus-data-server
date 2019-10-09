package com.ncc.neon.controllers;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.ExportQuery;
import com.ncc.neon.models.queries.FieldNamePrettyNamePair;
import com.ncc.neon.models.results.ExportResult;
import com.ncc.neon.models.results.TabularQueryResult;
import com.ncc.neon.services.QueryService;
import com.ncc.neon.util.CoreUtil;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import reactor.core.publisher.Mono;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("exportservice")
@Slf4j
public class ExportController {
    private QueryService queryService;
    
    ExportController(QueryService queryService) {
        this.queryService = queryService;
    }

    private String[] GetCSVHeader(List<FieldNamePrettyNamePair> fieldNamePrettyNamePairs)
    {
        return fieldNamePrettyNamePairs.stream().map(pair -> pair.getPretty()).collect(Collectors.toList()).toArray(String[]::new);
    }

    private String[] GetCSVRecord(Map<String, Object> map, List<FieldNamePrettyNamePair> fieldNamePrettyNamePairs)
    {
        List<String> fieldNames = fieldNamePrettyNamePairs.stream().map(pair -> pair.getQuery()).collect(Collectors.toList());
        String[] csvRecord = new String[fieldNames.size()];

        for(int index = 0; index < fieldNames.size(); index++)
        {
            String fieldName = fieldNames.get(index);
            Object value = CoreUtil.deepFind(map, fieldName).toString();
            csvRecord[index] = value.toString();
        }
        return csvRecord;
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

        if (exportQuery.getFieldNamePrettyNamePairs().isEmpty() || 
            exportQuery.getDataStoreType().trim().isEmpty() || 
            exportQuery.getHostName().trim().isEmpty())
        {
            return Mono.just(ResponseEntity.badRequest().body(new ExportResult(exportQuery.getFileName(), "missing hostName, dataStoreType or queryFieldNameMap")));
        }

        ConnectionInfo ci = new ConnectionInfo(exportQuery.getDataStoreType(), exportQuery.getHostName());
        Mono<TabularQueryResult> monoResult = queryService.executeQuery(ci, exportQuery.getQuery());

        Writer writer = new StringWriter();
        
        ICSVWriter csvWriter = new CSVWriterBuilder(writer).build();        

        //add header
        csvWriter.writeNext(GetCSVHeader(exportQuery.getFieldNamePrettyNamePairs()));

        //add data records
        monoResult
        .map(result -> {
            return result.getData();           
        })
        .subscribe(records -> {
            records.forEach(record -> csvWriter.writeNext(GetCSVRecord(record, exportQuery.getFieldNamePrettyNamePairs())));
        });

        ExportResult exportResult = new ExportResult(String.format("%s.csv", exportQuery.getFileName()), writer.toString());
        return Mono.just(ResponseEntity.ok()
        .body(exportResult));       

    }    
}

