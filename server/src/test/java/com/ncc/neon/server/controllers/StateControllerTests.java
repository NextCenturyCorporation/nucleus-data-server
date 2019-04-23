package com.ncc.neon.server.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.core.publisher.Mono;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureWebTestClient
public class StateControllerTests {

    private static String STATE_DIRECTORY = "src/test/resources/states";

    @Autowired
    private WebTestClient webClient;

    @Test
    public void testDeleteState() {
        try {
            File testFile = new File(STATE_DIRECTORY + "/testStateName.yaml");
            testFile.createNewFile();
            assertThat(testFile.exists()).isEqualTo(true);

            this.webClient.get()
                .uri("/stateservice/deletestate/testStateName")
                .accept(MediaType.APPLICATION_JSON_UTF8)
                .exchange()
                .expectStatus()
                .isOk()
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .expectBody(Boolean.class)
                .consumeWith(result -> {
                    Boolean successful = result.getResponseBody();
                    assertThat(successful).isEqualTo(true);

                    assertThat(testFile.exists()).isEqualTo(false);
                });
        }
        catch (IOException e) {
            fail(e.toString());
        }
    }

    @Test
    public void testFindStateNames() {
        this.webClient.get()
            .uri("/stateservice/allstatesnames")
            .accept(MediaType.APPLICATION_JSON_UTF8)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody(List.class)
            .consumeWith(result -> {
                List<String> actual = result.getResponseBody();
                assertThat(actual.toArray()).isEqualTo(new String[] {
                    "jsonState1", "jsonState2", "jsonState3", "jsonState4", "jsonState5", "jsonState6", "jsonState7",
                    "yamlState1", "yamlState2", "yamlState3", "yamlState4", "yamlState5", "yamlState6", "yamlState7"
                });
            });
    }

    @Test
    public void testLoadState() {
        this.webClient.get()
            .uri("/stateservice/loadstate?stateName=jsonState3")
            .accept(MediaType.APPLICATION_JSON_UTF8)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody(Map.class)
            .consumeWith(result -> {
                Map actual = result.getResponseBody();
                assertThat(actual).isEqualTo(new LinkedHashMap(Map.ofEntries(
                    Map.entry("list", Arrays.asList("a", "b", "c" )),
                    Map.entry("number", 123),
                    Map.entry("object", Map.ofEntries(Map.entry("key", "value"))),
                    Map.entry("string", "test")
                )));
            });
    }

    @Test
    public void testSaveState() {
        File testFile = new File(STATE_DIRECTORY + "/testStateName.yaml");
        assertThat(testFile.exists()).isEqualTo(false);

        Map<String, String> testBody = Map.ofEntries(Map.entry("key", "value"));

        this.webClient.post()
            .uri("/stateservice/savestate?stateName=testStateName")
            .accept(MediaType.APPLICATION_JSON_UTF8)
            .body(Mono.just(testBody), Map.class)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody(Boolean.class)
            .consumeWith(result -> {
                Boolean successful = result.getResponseBody();
                assertThat(successful).isEqualTo(true);
                assertThat(testFile.exists()).isEqualTo(true);

                try {
                    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
                    Map expected = yamlMapper.readValue("key: value", LinkedHashMap.class);
                    Map actual = yamlMapper.readValue(testFile, LinkedHashMap.class);
                    assertThat(actual).isEqualTo(expected);
                }
                catch (IOException e) {
                    fail(e.toString());
                }

                testFile.delete();
                assertThat(testFile.exists()).isEqualTo(false);
            });
    }
}
