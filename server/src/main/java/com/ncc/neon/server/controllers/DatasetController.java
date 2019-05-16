package com.ncc.neon.server.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@RestController
@RequestMapping("dataset")
@Slf4j
public class DatasetController {

    private long lastUpdated = 0l;

    private Flux<Long> stream;
    private FluxSink<Long> sink;

    DatasetController() {
        this.stream = Flux.create((FluxSink<Long> sink) -> {
            this.sink = sink;
        });
    }

    /**
     * Endpoint to be used to notify any listeners, that the datasets have been
     * updated
     */
    @PostMapping(path = "notify")
    @ResponseStatus(code = HttpStatus.ACCEPTED)
    void notifyChange() {
        this.lastUpdated = System.currentTimeMillis();
        this.sink.next(this.lastUpdated);
        return;
    }

    @GetMapping(path = "listen", produces = "text/event-stream")
    public Flux<Long> listen() {
        return this.stream;
    }
}