package com.ncc.neon.controllers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import java.util.List;

import com.ncc.neon.NeonServerApplication;
import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.ImportQuery;
import com.ncc.neon.models.results.ActionResult;
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

import reactor.core.publisher.Flux;
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

    @Test
    public void importFromCSV_shouldReturnBadRequestForInvalidInput() {
        ImportQuery importQuery = new ImportQuery();
        webTestClient.post()
                .uri("/importservice/")
				.contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(importQuery), ImportQuery.class)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    public void importFromCSV_shouldReturnDataForValidInput()
    {
        List<String> source = List.of("record1", "record2");
        ImportQuery importQuery = new ImportQuery("testHost", "testDataStoreType", "testDatabase", "testTable", source, false);

        ActionResult importResult = new ActionResult("One record failed to import");

        ConnectionInfo ci = new ConnectionInfo(importQuery.getDataStoreType(), importQuery.getHostName());

        when(queryService.getDatabaseNames(ci)).thenReturn(Flux.just("testDatabase"));
        when(queryService.getTableNames(ci, "testDatabase")).thenReturn(Flux.just("testTable"));
        when(queryService.importData(ci, importQuery)).thenReturn(Mono.just(importResult));

        webTestClient.post()
                .uri("/importservice/")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(importQuery), ImportQuery.class)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .expectBody(ActionResult.class)
                .value(result -> {
                    assertEquals(importResult.getError(), result.getError());
                });
    }
}