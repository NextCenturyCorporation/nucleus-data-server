package com.ncc.neon.server.controllers;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.core.publisher.Flux;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment =  WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
public class DatasetControllerTests {

    @Autowired
    private WebTestClient webClient;

    @Autowired
    private TestRestTemplate restTemplate;


    @Test
    public void getSubscriptions() {
        final TestRestTemplate tpl = this.restTemplate;

        Flux.interval(Duration.ofMillis(100), Duration.ofMillis(20)).take(3).subscribe(it -> {
            tpl.execute("/dataset/notify", HttpMethod.POST, null, null);            
        });


        List<Long> timestamps = this.webClient.get()
            .uri("/dataset/listen")
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus().isOk()
            .returnResult(Long.class)
            .getResponseBody()
            .take(3)
            .collectList()
            .block(Duration.ofSeconds(1));


        assertEquals(timestamps.size(), 3);
        assertTrue(timestamps.get(0) < timestamps.get(1));
        assertTrue(timestamps.get(1) < timestamps.get(2));
    }
}
