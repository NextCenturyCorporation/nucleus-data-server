package com.ncc.neon.controllers;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ncc.neon.models.DataNotification;

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
    public void testListener() {
        Flux.interval(Duration.ofMillis(100), Duration.ofMillis(20)).take(3).subscribe(it -> {
            this.restTemplate.postForObject("/dataset/notify", Map.ofEntries(Map.entry("count", 10)), Map.class);
        });

        List<DataNotification> notifications = this.webClient.get()
            .uri("/dataset/listen")
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus().isOk()
            .returnResult(DataNotification.class)
            .getResponseBody()
            .take(3)
            .collectList()
            .block(Duration.ofSeconds(2));

        assertEquals(notifications.size(), 3);
        assertEquals(notifications.get(0).getCount(), 10l);
        assertEquals(notifications.get(1).getCount(), 10l);
        assertEquals(notifications.get(2).getCount(), 10l);
        assertTrue(notifications.get(0).getPublishDate() < notifications.get(1).getPublishDate());
        assertTrue(notifications.get(1).getPublishDate() < notifications.get(2).getPublishDate());
    }
}
