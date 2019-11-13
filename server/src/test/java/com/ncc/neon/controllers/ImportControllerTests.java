package com.ncc.neon.controllers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import java.util.List;

import com.ncc.neon.NeonServerApplication;
import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.ImportQuery;
import com.ncc.neon.models.results.ImportResult;
import com.ncc.neon.services.QueryService;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.core.publisher.Mono;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = NeonServerApplication.class)
@SpringBootTest
@AutoConfigureWebTestClient
public class ImportControllerTests {

    @Autowired
    WebTestClient webTestClient;

    @MockBean
    private QueryService queryService;

    @MockBean
    private ImportResult importResult;

    @Test
    public void importToCSV_shouldReturnBadRequestForInvalidInput() {
        ImportQuery importQuery = new ImportQuery();
        webTestClient.post()
                .uri("/importservice/")
				.contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(importQuery), ImportQuery.class)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    public void exportToCSV_shouldReturnDataForValidInput()
    {
        List<String> source = List.of("record1", "record2");       
        ImportQuery importQuery = new ImportQuery("testHost", "testDataStoreType", "tesetDatabase", "testTable", source);

        ImportResult importResult = new ImportResult(3, 1, "On record failed to import");

        ConnectionInfo ci = new ConnectionInfo(importQuery.getDataStoreType(), importQuery.getHostName());
        when(queryService.addData(ci, importQuery.getDatabase(), importQuery.getTable(), importQuery.getSource())).thenReturn(Mono.just(importResult));

        webTestClient.post()
                .uri("/importservice/")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(importQuery), ImportQuery.class)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .expectBody(ImportResult.class)
                .value(result -> {
                    assertEquals(importResult.getTotal(), result.getTotal());
                    assertEquals(importResult.getFailCount(), result.getFailCount());
                    assertEquals(importResult.getError(), result.getError());
                });
    }
}