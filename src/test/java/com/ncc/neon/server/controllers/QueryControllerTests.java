package com.ncc.neon.server.controllers;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * QueryControllerTests
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureWebTestClient
public class QueryControllerTests {

    @Autowired
    private WebTestClient webClient;

    @Test
    public void exampleTest() {

        this.webClient.get()
                .uri("/queryservice/tablesandfields/localhost/elasticsearchrest/ldc_uyg_jul_18")
                .accept(MediaType.APPLICATION_JSON_UTF8)
                .exchange()
                .expectStatus()
                .isOk()
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .expectBody(Map.class)
                .consumeWith(result -> {
                    // Good map assertions examples
                    // https://github.com/joel-costigliola/assertj-examples/blob/master/assertions-examples/src/test/java/org/assertj/examples/MapAssertionsExamples.java
                    Map<String, List<String>> map = result.getResponseBody();
                    Assertions.assertThat(map).isNotEmpty().hasSize(2);
                    Assertions.assertThat(map).containsKey("1");
                });
    }

}