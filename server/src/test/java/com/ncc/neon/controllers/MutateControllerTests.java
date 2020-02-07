package com.ncc.neon.controllers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.LinkedHashMap;

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

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = NeonServerApplication.class)
@SpringBootTest
@AutoConfigureWebTestClient
public class MutateControllerTests {

    @Autowired
    WebTestClient webTestClient;

    @MockBean
    private QueryService queryService;

    @Test
    public void testMutateWithInvalidInput() {
        MutateQuery mutateQuery = new MutateQuery("", "", "", "", "", "", new LinkedHashMap<String, Object>());
        webTestClient.post()
                .uri("/mutateservice/byid")
				.contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(mutateQuery), MutateQuery.class)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    public void testMutateWithValidInput() {
        MutateQuery mutateQuery = new MutateQuery("testHost", "testType", "testDatabase", "testTable", "testIdField",
            "testId", new LinkedHashMap<String, Object>(){{
                put("testStringField", "testStringValue");
            }}
        );

        ActionResult mutateResult = new ActionResult("One record failed to import");

        ConnectionInfo info = new ConnectionInfo(mutateQuery.getDatastoreType(), mutateQuery.getDatastoreHost());

        when(queryService.mutateData(info, mutateQuery)).thenReturn(Mono.just(mutateResult));

        webTestClient.post()
                .uri("/mutateservice/byid")
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
