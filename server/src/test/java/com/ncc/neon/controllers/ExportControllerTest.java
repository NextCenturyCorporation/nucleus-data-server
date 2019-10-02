package com.ncc.neon.controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.ExportQuery;
import com.ncc.neon.models.queries.FieldNamePrettyNamePair;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.ExportResult;
import com.ncc.neon.models.results.TabularQueryResult;
import com.ncc.neon.services.QueryService;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.core.publisher.Mono;

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Arrays;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureWebTestClient
public class ExportControllerTest {

    @Autowired
    WebTestClient webTestClient;

    @MockBean
    private QueryService queryService;

    @MockBean
    private TabularQueryResult queryResult;

    @Test
    public void exportToCSV_shouldReturnBadRequestForInvalidInput() {
        ExportQuery exportQuery = new ExportQuery("test.csv", "elasticsearch", "loaclhost", new Query(), new ArrayList<FieldNamePrettyNamePair>());
        webTestClient.post()
                .uri("/exportservice/csv")
				.contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(exportQuery), ExportQuery.class)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    public void exportToCSV_shouldReturnOnlyHeaderRowWhenNoData()
    {
        List<FieldNamePrettyNamePair> fieldNamePrettyNamePairs = 
        List.of(new FieldNamePrettyNamePair("firstName", "first name"), new FieldNamePrettyNamePair("lastName", "last name"));
        

        ExportQuery exportQuery = new ExportQuery("test.csv", "elasticsearch", "loaclhost", new Query(), fieldNamePrettyNamePairs);

        when(queryResult.getData()).thenReturn(new ArrayList<Map<String, Object>>());
        Mono<TabularQueryResult> queryResultMono = Mono.just(queryResult);

        ConnectionInfo ci = new ConnectionInfo(exportQuery.getDataStoreType(), exportQuery.getHostName());
        when(queryService.executeQuery(ci, exportQuery.getQuery())).thenReturn(queryResultMono);

        webTestClient.post()
                .uri("/exportservice/csv")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(exportQuery), ExportQuery.class)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .expectBody(ExportResult.class)
                .value(exportResult -> {
                    assertEquals("test.csv", exportResult.getFileName());

                    //there should be header row only
                    String[] csvRecords = exportResult.getData().toString().split(System.lineSeparator());
                    Assertions.assertThat(csvRecords.length).isEqualTo(1);

                    String[] headerArray = csvRecords[0].split(",");
                    List<Object> headerRow = Arrays.asList(headerArray);
                    assertTrue(headerRow.contains("last name"));
                    assertTrue(headerRow.contains("first name"));                    
                });

    }

    @Test
    public void exportToCSV_dataRowFieldOrdersShouldAlignWithHeaderRow()
    {
        List<FieldNamePrettyNamePair> fieldNamePrettyNamePairs = 
        List.of(new FieldNamePrettyNamePair("firstName", "first name"), new FieldNamePrettyNamePair("lastName", "last name"), new FieldNamePrettyNamePair("age", "age"));

        ExportQuery exportQuery = new ExportQuery("test.csv", "elasticsearch", "loaclhost", new Query(), fieldNamePrettyNamePairs);

        List<Map<String, Object>> data = List.of(
            Map.of("firstName", "John", "lastName", "Doe", "age", 30),
            Map.of("firstName", "Jane", "lastName", "Doe", "age", 40));

        when(queryResult.getData()).thenReturn(data);
        Mono<TabularQueryResult> queryResultMono = Mono.just(queryResult);

        ConnectionInfo ci = new ConnectionInfo(exportQuery.getDataStoreType(), exportQuery.getHostName());
        when(queryService.executeQuery(ci, exportQuery.getQuery())).thenReturn(queryResultMono);

        webTestClient.post()
                .uri("/exportservice/csv")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(exportQuery), ExportQuery.class)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .expectBody(ExportResult.class)
                .value(
                    exportResult -> 
                    {   
                        assertEquals("test.csv", exportResult.getFileName());

                        //validate row count including header row
                        String[] csvRecords = exportResult.getData().toString().split(System.lineSeparator());
                        Assertions.assertThat(csvRecords.length).isEqualTo(3);

                        String[] mapRecordKeys = csvRecords[0].split(",");
                       
                        //validate first record values
                        String[] firstRecordValues = csvRecords[1].split(",");
                        Map<String, String> firstMapRecord = new HashMap<>();
                        firstMapRecord.put(mapRecordKeys[0], firstRecordValues[0]);
                        firstMapRecord.put(mapRecordKeys[1], firstRecordValues[1]);
                        firstMapRecord.put(mapRecordKeys[2], firstRecordValues[2]);

                        assertTrue(firstMapRecord.containsKey("first name"));
                        assertTrue(firstMapRecord.containsKey("last name"));
                        assertTrue(firstMapRecord.containsKey("age"));
                    
                        assertEquals("John", firstMapRecord.get("first name"));
                        assertEquals("Doe", firstMapRecord.get("last name"));
                        assertEquals("30", firstMapRecord.get("age"));


                        //validate second record values
                        String[] secondRecordValues = csvRecords[2].split(",");
                        Map<String, String> secondMapRecord = new HashMap<>();
                        secondMapRecord.put(mapRecordKeys[0], secondRecordValues[0]);
                        secondMapRecord.put(mapRecordKeys[1], secondRecordValues[1]);
                        secondMapRecord.put(mapRecordKeys[2], secondRecordValues[2]);

                        assertTrue(secondMapRecord.containsKey("first name"));
                        assertTrue(secondMapRecord.containsKey("last name"));
                        assertTrue(secondMapRecord.containsKey("age"));
                    
                        assertEquals("Jane", secondMapRecord.get("first name"));
                        assertEquals("Doe", secondMapRecord.get("last name"));
                        assertEquals("40", secondMapRecord.get("age"));

                    }
                );
    }    
}