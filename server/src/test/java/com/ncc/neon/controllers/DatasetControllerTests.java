package com.ncc.neon.controllers;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import com.ncc.neon.models.DataNotification;
import com.ncc.neon.util.DateUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.FluxExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment =  WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
public class DatasetControllerTests {

    @Autowired
    private WebTestClient webClient;

    @Autowired
    private TestRestTemplate restTemplate;

    private ZonedDateTime lastDate;

    private boolean validateNotification(DataNotification notification, long count) {
        ZonedDateTime nextDate = DateUtil.transformStringToDate(notification.getTimestamp());
        boolean validation = notification.getCount() == count && (lastDate.compareTo(nextDate) < 0);
        lastDate = nextDate;
        return validation;
    }

    @Test
    public void testNotify() {
        lastDate = ZonedDateTime.now();

        this.webClient.post()
            .uri("/dataset/notify")
            .accept(MediaType.APPLICATION_JSON_UTF8)
            .body(Mono.just(Map.ofEntries(Map.entry("count", 1234l))), Map.class)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody(String.class)
            .consumeWith(result -> {
                assertTrue(lastDate.compareTo(DateUtil.transformStringToDate(result.getResponseBody())) < 0);
            });
    }

    @Test
    public void testListen() {
        lastDate = ZonedDateTime.now();

        Flux.interval(Duration.ofMillis(100), Duration.ofMillis(20)).take(3).subscribe(index -> {
            this.restTemplate.postForObject("/dataset/notify", Map.ofEntries(Map.entry("count", 100 + index)), String.class);
        });

        FluxExchangeResult<ServerSentEvent<DataNotification>> notifications = this.webClient.get()
            .uri("/dataset/listen")
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus().isOk()
            .returnResult(new ParameterizedTypeReference<ServerSentEvent<DataNotification>>() {});

        StepVerifier.create(notifications.getResponseBody().take(3))
            .consumeNextWith(e -> assertTrue(validateNotification(e.data(), 100l)))
            .consumeNextWith(e -> assertTrue(validateNotification(e.data(), 101l)))
            .consumeNextWith(e -> assertTrue(validateNotification(e.data(), 102l)))
            .thenCancel()
            .verify();
    }
}
