package com.ncc.neon.controllers;

import com.ncc.neon.NeonServerApplication;
import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.MutateQuery;
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
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = NeonServerApplication.class)
@SpringBootTest
@AutoConfigureWebTestClient
public class DeleteControllerTests {

    @Autowired
    WebTestClient webTestClient;

    @MockBean
    private QueryService queryService;

    @Test
    public void testDeleteWithInvalidInput() {
        MutateQuery mutateQuery = new MutateQuery("", "", "", "", "", "", new LinkedHashMap<String, Object>());
        webTestClient.post()
                .uri("/deleteservice/delete")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(mutateQuery), MutateQuery.class)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    public void testDeleteWithValidInput() {
        MutateQuery mutateQuery = new MutateQuery("testHost", "testType", "testDatabase", "testTable", "testIdField",
                "testId", new LinkedHashMap<String, Object>(){{
            put("testStringField", "testStringValue");
        }}
        );

        ActionResult mutateResult = new ActionResult("1 row deleted in testDatabase.testTable");

        ConnectionInfo info = new ConnectionInfo(mutateQuery.getDatastoreType(), mutateQuery.getDatastoreHost());

        when(queryService.deleteData(info, mutateQuery)).thenReturn(Mono.just(mutateResult));

        webTestClient.post()
                .uri("/deleteservice/delete")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(mutateQuery), MutateQuery.class)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .expectBody(ActionResult.class)
                .value(result -> {
                    assertEquals(mutateResult, result);
                });
    }
}
