package com.ncc.neon.controllers;

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.ExportQuery;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.TabularQueryResult;
import com.ncc.neon.services.QueryService;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.ContentDisposition;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.core.publisher.Mono;

import org.assertj.core.api.Assertions;

@RunWith(SpringRunner.class)
//@WebFluxTest(ExportController.class)
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
    public void shouldReturnBadRequestForInvalidInput() {
        ExportQuery exportQuery = new ExportQuery("test.csv", "elasticsearch", "loaclhost", new Query(), new HashMap<String, String>());
/*
        TabularQueryResult queryResult = new TabularQueryResult();
        
        Mono<TabularQueryResult> queryResult 

        Mono<ExportQuery> employeeMono = Mono.just(employee);

        when(employeeService.getEmployeeById(1)).thenReturn(employeeMono);
*/
        webTestClient.post()
                .uri("/exportservice/csv")
				.contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(exportQuery), ExportQuery.class)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    public void shouldReturnOnlyHeaderRowWhenNoData()
    {
        Map<String, String> queryFieldNameMap = Map.of("firstName","first name", "lastName", "last name");

        ExportQuery exportQuery = new ExportQuery("test.csv", "elasticsearch", "loaclhost", new Query(), queryFieldNameMap);

        when(queryResult.getData()).thenReturn(new ArrayList<Map<String, Object>>());
        Mono<TabularQueryResult> queryResultMono = Mono.just(queryResult);

        ConnectionInfo ci = new ConnectionInfo(exportQuery.getDatabaseType(), exportQuery.getHostName());
        when(queryService.executeQuery(ci, exportQuery.getQuery())).thenReturn(queryResultMono);

        webTestClient.post()
                .uri("/exportservice/csv")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(exportQuery), ExportQuery.class)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.TEXT_PLAIN_VALUE)
                .expectHeader().contentDisposition(ContentDisposition.parse("attachment; filename=\"" + exportQuery.getFileName() + "\""))
                .expectBody()
                .consumeWith(
                    response -> 
                    {   
                        String responseBody = new String(response.getResponseBody());
                        Assertions.assertThat(responseBody).isEqualTo("first name,last name");
                    }
                );
                
    }
}