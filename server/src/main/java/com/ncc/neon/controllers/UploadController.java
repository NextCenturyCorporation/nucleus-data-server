package com.ncc.neon.controllers;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.UploadQuery;
import com.ncc.neon.models.queries.FieldNamePrettyNamePair;
import com.ncc.neon.models.results.ExportResult;
import com.ncc.neon.models.results.TabularQueryResult;
import com.ncc.neon.services.QueryService;
import com.opencsv.CSVWriter;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("uploadData")
public class UploadController {
    private QueryService queryService;

    UploadController(QueryService queryService) {
        this.queryService = queryService;
    }

    /**
     * Executes a query against the supplied connection and returns the result in CSV format
     * 
     * @param uploadQuery  upload parameters
     * @return The result of the upload containing the upload file name and the upload data in csv formt
     */
    public Mono<Boolean> addData(@RequestBody UploadQuery uploadQuery, String databaseName, String table, TabularQueryResult sourceData) throws IOException
    {
        //log.debug("Upload parameters: " + uploadQuery.toString());

        ConnectionInfo ci = new ConnectionInfo(uploadQuery.getDataStoreType(), uploadQuery.getHostName());

        Mono<Boolean> uploadSuccessful = queryService.addData(ci, databaseName, table, sourceData);

        return uploadSuccessful;
    }    
}
