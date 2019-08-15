package com.ncc.neon.controllers;

import java.time.Duration;
import java.time.ZonedDateTime;

import com.ncc.neon.models.DataNotification;
import com.ncc.neon.util.DateUtil;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@CrossOrigin(origins="*")
@RestController
@RequestMapping("dataset")
@Slf4j
public class DatasetController {

    final private FluxProcessor<DataNotification, DataNotification> processor;
    final private FluxSink<DataNotification> sink;

    DatasetController() {
        processor = DirectProcessor.<DataNotification>create().serialize();
        sink = processor.sink();
    }

    /**
     * Endpoint to be used to notify any listeners, that the datasets have been
     * updated
     */
    @PostMapping(path = "notify")
    @ResponseStatus(code = HttpStatus.ACCEPTED)
    public ResponseEntity<Mono<String>> notifyChange(@RequestBody() DataNotification notification) {
        if (notification.getTimestamp() == null)  {
            notification.setTimestamp(DateUtil.transformDateToString(ZonedDateTime.now()));
        }
        sink.next(notification);
        return ResponseEntity.ok().body(Mono.just(notification.getTimestamp()));
    }

    private Flux<ServerSentEvent<DataNotification>> getHeartbeatStream() {
        return Flux.interval(Duration.ofSeconds(30))
            .map(i -> ServerSentEvent.<DataNotification>builder().build());
    }

    /**
     * Subscribe to data set change notifications
     */
    @GetMapping(path = "listen", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<Flux<ServerSentEvent<DataNotification>>> listen() {
        return ResponseEntity.ok().body(Flux.merge(
            getHeartbeatStream(),
            processor.map(e -> ServerSentEvent.<DataNotification>builder(e).build())
        ));
    }
}
