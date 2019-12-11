package com.ncc.neon.services;

import java.time.Duration;
import java.time.ZonedDateTime;

import com.ncc.neon.models.DataNotification;
import com.ncc.neon.util.DateUtil;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class DatasetService {

    final private FluxProcessor<DataNotification, DataNotification> processor;
    final private FluxSink<DataNotification> sink;

    DatasetService() {
        processor = DirectProcessor.<DataNotification>create().serialize();
        sink = processor.sink();
    }

    /**
     * Subscribes to data notifications and returns the ServerSentEvent listener object.
     */
    public Flux<ServerSentEvent<DataNotification>> listen() {
        return Flux.merge(
            createHeartbeatStream(),
            processor.map(e -> ServerSentEvent.<DataNotification>builder(e).build())
        );
    }

    /**
     * Sends the given data notification object to all its listeners and returns the timestamp.
     */
    public Mono<String> notify(DataNotification notification) {
        if (notification.getTimestamp() == null)  {
            notification.setTimestamp(DateUtil.transformDateToString(ZonedDateTime.now()));
        }
        log.debug("sending notification");
        sink.next(notification);
        return Mono.just(notification.getTimestamp());
    }

    private Flux<ServerSentEvent<DataNotification>> createHeartbeatStream() {
        return Flux.interval(Duration.ofSeconds(30))
            .map(i -> ServerSentEvent.<DataNotification>builder().build());
    }
}
