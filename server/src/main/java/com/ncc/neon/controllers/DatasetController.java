package com.ncc.neon.controllers;

import java.util.ArrayList;
import java.util.List;

import com.ncc.neon.models.DataNotification;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@CrossOrigin(origins="*")
@RestController
@RequestMapping("dataset")
@Slf4j
public class DatasetController {

    private long lastUpdated = 0l;

    private List<FluxSink<DataNotification>> listeners;

    DatasetController() {
        listeners = new ArrayList<FluxSink<DataNotification>>();
    }

    /**
     * Endpoint to be used to notify any listeners, that the datasets have been
     * updated
     */
    @PostMapping(path = "notify")
    @ResponseStatus(code = HttpStatus.ACCEPTED)
    void notifyChange(@RequestBody() DataNotification notification) {
        if (notification.getTimestamp() == 0)  {
            notification.setTimestamp(System.currentTimeMillis());
        }
        notification.setPublishDate(this.lastUpdated = System.currentTimeMillis()); 
        listeners.forEach(sink -> sink.next(notification));
        return;
    }

    /**
     * Subscribe to data set change notifications
     */
    @GetMapping(path = "listen", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<DataNotification> listen() {
        return Flux.create((FluxSink<DataNotification> sink) -> {
            listeners.add(sink);
        });
    }
}
